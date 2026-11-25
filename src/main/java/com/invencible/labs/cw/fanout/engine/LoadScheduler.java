package com.invencible.labs.cw.fanout.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.invencible.labs.cw.fanout.engine.FanOutStats.Counter;
import com.invencible.labs.cw.fanout.model.LoadException;
import com.invencible.labs.cw.fanout.model.LoadRequest;
import com.invencible.labs.cw.fanout.model.LoadResult;
import com.invencible.labs.cw.fanout.model.LoadTask;
import com.invencible.labs.cw.fanout.port.FileLoader;
import com.invencible.labs.cw.fanout.port.TaskStore;
import com.invencible.labs.cw.fanout.port.TaskStore.Claim;
import com.invencible.labs.cw.fanout.port.TaskStore.FailResult;

/**
 * Drains the task store into the slow downstream without ever hurting it.
 *
 * <p>One dispatcher thread decides <em>whether</em> a call may start, and a virtual thread per call does the blocking
 * work. A call may start only when all three guards agree:
 * <ol>
 *   <li>{@link CircuitBreaker}: the downstream is reachable.</li>
 *   <li>{@link AimdLimiter}: fewer calls in flight than the downstream is believed to handle.</li>
 *   <li>{@link TokenBucket}: the start rate is within budget (and no Retry-After pause is active).</li>
 * </ol>
 * Only then does it ask the {@link TaskStore} for work, which decides <em>which</em> task goes next (fairness).
 * The limit is a semantic limit, not a thread-pool size. Virtual threads make 100 blocked one-minute calls cheap.
 *
 * <h2>How each outcome is treated</h2>
 * <pre>
 *  outcome      limiter    breaker   task
 *  success      grow       ok        done
 *  THROTTLED    halve      ok        back after max(Retry-After, backoff), attempt not counted, bucket paused
 *  TIMEOUT      trim 10%   failure   retry with backoff (attempt counted)
 *  UNAVAILABLE  -          failure   retry with backoff (attempt counted)
 *  REJECTED     -          ok        dead-lettered at once (retrying cannot help)
 * </pre>
 * What protects the queue during an outage is the breaker, not the attempt accounting: while it is open nothing is
 * called, so nothing fails and no attempt is spent. Only the wave that was already in flight when it tripped, and the
 * occasional probe, cost their tasks one attempt each.
 */
public final class LoadScheduler implements AutoCloseable {

	/**
	 * @param tick          how often the dispatcher re-checks when it cannot make progress
	 * @param callTimeout   the deadline handed to the downstream for each call
	 * @param shutdownGrace how long {@link #close} lets running calls finish before interrupting them
	 */
	public record Options(Duration tick, Duration callTimeout, Duration shutdownGrace) {
	}

	private static final Logger log = LoggerFactory.getLogger(LoadScheduler.class);

	private final TaskStore store;
	private final FileLoader loader;
	private final LoadTaskHandler handler;
	private final AimdLimiter limiter;
	private final TokenBucket bucket;
	private final CircuitBreaker breaker;
	private final Backoff backoff;
	private final Options options;
	private final Clock clock;
	private final FanOutStats stats;
	private final ExecutorService calls = Executors.newVirtualThreadPerTaskExecutor();

	private volatile boolean running;
	private Thread dispatcher;

	public LoadScheduler(TaskStore store, FileLoader loader, LoadTaskHandler handler, AimdLimiter limiter,
			TokenBucket bucket, CircuitBreaker breaker, Backoff backoff, Options options, Clock clock,
			FanOutStats stats) {
		this.store = store;
		this.loader = loader;
		this.handler = handler;
		this.limiter = limiter;
		this.bucket = bucket;
		this.breaker = breaker;
		this.backoff = backoff;
		this.options = options;
		this.clock = clock;
		this.stats = stats;
	}

	public synchronized void start() {
		if (dispatcher != null) {
			throw new IllegalStateException("already started");
		}
		running = true;
		dispatcher = Thread.ofPlatform().name("fanout-dispatcher").start(this::dispatchLoop);
	}

	/** Blocks until nothing is queued or in flight (delayed retries count as queued), or the timeout passes. */
	public boolean awaitIdle(Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (System.nanoTime() < deadline) {
			if (store.counts().idle()) {
				return true;
			}
			Thread.sleep(10);
		}
		return store.counts().idle();
	}

	/** Graceful stop: no new calls, running calls get a grace period, then they are interrupted and re-queued. */
	@Override
	public void close() {
		running = false;
		wake();
		try {
			Thread d = dispatcher;
			if (d != null) {
				d.join(options.shutdownGrace().toMillis());
			}
			calls.shutdown();
			if (!calls.awaitTermination(options.shutdownGrace().toMillis(), TimeUnit.MILLISECONDS)) {
				log.warn("calls still running after {}: interrupting them, their tasks go back to the queue",
						options.shutdownGrace());
				calls.shutdownNow();
				calls.awaitTermination(5, TimeUnit.SECONDS);
			}
		}
		catch (InterruptedException e) {
			calls.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	// ---------------------------------------------------------------- dispatcher

	private void dispatchLoop() {
		while (running) {
			try {
				int expired = store.reclaimExpired();
				if (expired > 0) {
					stats.add(Counter.LEASES_EXPIRED, expired);
					log.warn("{} expired leases returned to the queue", expired);
				}
				Optional<Slot> slot = tryAdmit();
				if (slot.isEmpty()) {
					park(admissionWait());
					continue;
				}
				Optional<Claim> claim = store.claimNext();
				if (claim.isEmpty()) {
					slot.get().cancel();
					park(options.tick());
					continue;
				}
				stats.observeInFlight(limiter.inFlight());
				launch(claim.get(), slot.get());
			}
			catch (RuntimeException e) {
				log.error("dispatcher iteration failed, continuing", e);
				park(options.tick());
			}
		}
	}

	/** All three guards must agree. Anything already taken is given back if a later guard says no. */
	private Optional<Slot> tryAdmit() {
		Optional<CircuitBreaker.Permit> breakerPermit = breaker.tryAcquire();
		if (breakerPermit.isEmpty()) {
			return Optional.empty();
		}
		Optional<AimdLimiter.Permit> limiterPermit = limiter.tryAcquire();
		if (limiterPermit.isEmpty()) {
			breakerPermit.get().cancel();
			return Optional.empty();
		}
		if (!bucket.tryTake()) {
			limiterPermit.get().neutral();
			breakerPermit.get().cancel();
			return Optional.empty();
		}
		return Optional.of(new Slot(breakerPermit.get(), limiterPermit.get()));
	}

	private Duration admissionWait() {
		Duration wait = options.tick();
		for (Duration hint : new Duration[] { breaker.remainingOpen(), bucket.waitTime() }) {
			if (!hint.isZero() && hint.compareTo(wait) < 0) {
				wait = hint;
			}
		}
		return wait;
	}

	private void launch(Claim claim, Slot slot) {
		try {
			calls.execute(() -> run(claim, slot));
		}
		catch (RuntimeException e) {
			slot.cancel();
			store.defer(claim, clock.instant());
			throw e;
		}
	}

	private void park(Duration duration) {
		LockSupport.parkNanos(this, Math.max(1_000_000L, duration.toNanos()));
	}

	private void wake() {
		Thread d = dispatcher;
		if (d != null) {
			LockSupport.unpark(d);
		}
	}

	// ---------------------------------------------------------------- one call

	private void run(Claim claim, Slot slot) {
		try {
			execute(claim, slot);
		}
		catch (Exception e) {
			log.error("unexpected failure while running {}", claim.task().key(), e);
			slot.release();
			retryLater(claim, "unexpected: " + e);
		}
		finally {
			slot.release();
			wake();
		}
	}

	private void execute(Claim claim, Slot slot) {
		LoadTask task = claim.task();
		if (!stillNeeded(claim, slot)) {
			return;
		}
		LoadResult result;
		try {
			result = loader.load(new LoadRequest(task.fileId(), task.key().toString(), options.callTimeout()));
		}
		catch (LoadException e) {
			onLoadFailure(claim, slot, e);
			return;
		}
		catch (InterruptedException e) {
			slot.cancel();
			store.defer(claim, clock.instant());
			Thread.currentThread().interrupt();
			return;
		}
		catch (RuntimeException e) {
			onLoadFailure(claim, slot, LoadException.unavailable("loader threw " + e, e));
			return;
		}

		slot.success();
		stats.inc(Counter.LOADS_OK);
		try {
			handler.onLoaded(task, result);
			if (!store.complete(claim)) {
				log.warn("lease of {} was lost during the call: another worker owns the task now", task.key());
			}
		}
		catch (RuntimeException e) {
			stats.inc(Counter.POST_LOAD_FAILURES);
			retryLater(claim, "post-load step failed: " + e);
		}
	}

	/** Check before act: skip a call that has become pointless while the task waited. */
	private boolean stillNeeded(Claim claim, Slot slot) {
		boolean needed;
		try {
			needed = handler.isStillNeeded(claim.task());
		}
		catch (RuntimeException e) {
			slot.cancel();
			retryLater(claim, "pre-check failed: " + e);
			return false;
		}
		if (!needed) {
			slot.cancel();
			store.complete(claim);
			stats.inc(Counter.LOADS_SKIPPED_NOT_NEEDED);
		}
		return needed;
	}

	private void onLoadFailure(Claim claim, Slot slot, LoadException e) {
		Instant now = clock.instant();
		switch (e.kind()) {
			case THROTTLED -> {
				slot.throttled();
				bucket.pauseUntil(now.plus(e.retryAfter()));
				Duration wait = e.retryAfter().compareTo(backoff.delay(claim.attempt())) > 0
						? e.retryAfter() : backoff.delay(claim.attempt());
				store.defer(claim, now.plus(wait));
				stats.inc(Counter.LOADS_THROTTLED);
			}
			case TIMEOUT -> {
				slot.timedOut();
				stats.inc(Counter.LOADS_TIMED_OUT);
				retryLater(claim, "timeout: " + e.getMessage());
			}
			case UNAVAILABLE -> {
				slot.unavailable();
				stats.inc(Counter.LOADS_UNAVAILABLE);
				retryLater(claim, "unavailable: " + e.getMessage());
			}
			case REJECTED -> {
				slot.rejected();
				stats.inc(Counter.LOADS_REJECTED);
				stats.inc(Counter.DEAD_LETTERED);
				store.deadLetter(claim, "rejected: " + e.getMessage());
				log.warn("dead-lettered {} (permanent failure): {}", claim.task().key(), e.getMessage());
			}
		}
	}

	private void retryLater(Claim claim, String reason) {
		Instant notBefore = clock.instant().plus(backoff.delay(claim.attempt()));
		FailResult result = store.fail(claim, notBefore, reason);
		switch (result) {
			case RETRY -> stats.inc(Counter.RETRIES);
			case DEAD -> {
				stats.inc(Counter.DEAD_LETTERED);
				log.warn("dead-lettered {} after exhausting its attempts: {}", claim.task().key(), reason);
			}
			case STALE -> log.warn("ignored failure of {}: its lease was lost", claim.task().key());
		}
	}

	/** The three permits of one call, completed together so that none can leak. */
	private final class Slot {
		private final CircuitBreaker.Permit breakerPermit;
		private final AimdLimiter.Permit limiterPermit;
		private final AtomicBoolean finished = new AtomicBoolean();

		Slot(CircuitBreaker.Permit breakerPermit, AimdLimiter.Permit limiterPermit) {
			this.breakerPermit = breakerPermit;
			this.limiterPermit = limiterPermit;
		}

		/** No call was made: give everything back, including the token. */
		void cancel() {
			if (finished.compareAndSet(false, true)) {
				bucket.refund();
				limiterPermit.neutral();
				breakerPermit.cancel();
			}
		}

		void success() {
			if (finished.compareAndSet(false, true)) {
				limiterPermit.success();
				breakerPermit.success();
			}
		}

		/** The downstream answered, but asked us to slow down. */
		void throttled() {
			if (finished.compareAndSet(false, true)) {
				limiterPermit.throttled();
				breakerPermit.success();
			}
		}

		void timedOut() {
			if (finished.compareAndSet(false, true)) {
				limiterPermit.timedOut();
				breakerPermit.failure();
			}
		}

		void unavailable() {
			if (finished.compareAndSet(false, true)) {
				limiterPermit.neutral();
				breakerPermit.failure();
			}
		}

		/** The downstream is healthy and said no to this file. */
		void rejected() {
			if (finished.compareAndSet(false, true)) {
				limiterPermit.neutral();
				breakerPermit.success();
			}
		}

		/** Safety net for unexpected exits. Says nothing about capacity and does not refund the token. */
		void release() {
			if (finished.compareAndSet(false, true)) {
				limiterPermit.neutral();
				breakerPermit.cancel();
			}
		}
	}
}
