package com.invencible.labs.cw.fanout.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stops calling a downstream that is down, and probes it carefully until it is back.
 *
 * <pre>
 *   CLOSED --N consecutive failures--> OPEN --open period elapsed--> HALF_OPEN --probe ok--> CLOSED
 *                                        ^                               |
 *                                        +----------probe failed---------+   (open period doubles, up to a cap)
 * </pre>
 *
 * <p>The AIMD limiter answers "how much can it take" and this answers "is it there at all". Without it a real outage
 * burns every queued task's retry budget in minutes.
 *
 * <p>Each state change starts a new epoch, and a permit is bound to the epoch it was issued in. A call that started
 * before the breaker tripped and fails after it cannot trip it again, and a call that started before the trip and
 * succeeds during HALF_OPEN cannot be mistaken for the probe.
 */
public final class CircuitBreaker {

	private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

	public enum State { CLOSED, OPEN, HALF_OPEN }

	private final int failureThreshold;
	private final Duration openFor;
	private final Duration maxOpenFor;
	private final Clock clock;

	private State state = State.CLOSED;
	private long epoch;
	private int consecutiveFailures;
	private boolean probeInFlight;
	private Instant openUntil = Instant.MIN;
	private Duration currentOpenFor;
	private long trips;

	public CircuitBreaker(int failureThreshold, Duration openFor, Duration maxOpenFor, Clock clock) {
		if (failureThreshold < 1) {
			throw new IllegalArgumentException("failureThreshold must be >= 1");
		}
		this.failureThreshold = failureThreshold;
		this.openFor = openFor;
		this.maxOpenFor = maxOpenFor;
		this.currentOpenFor = openFor;
		this.clock = clock;
	}

	/** Asks to make a call. Empty while OPEN, and while HALF_OPEN with a probe already out. */
	public synchronized Optional<Permit> tryAcquire() {
		Instant now = clock.instant();
		if (state == State.OPEN) {
			if (now.isBefore(openUntil)) {
				return Optional.empty();
			}
			moveTo(State.HALF_OPEN);
		}
		if (state == State.HALF_OPEN) {
			if (probeInFlight) {
				return Optional.empty();
			}
			probeInFlight = true;
			return Optional.of(new Permit(epoch, true));
		}
		return Optional.of(new Permit(epoch, false));
	}

	public synchronized State state() {
		return state;
	}

	public synchronized long trips() {
		return trips;
	}

	/** How long until a probe may go out, or zero when calls are allowed. */
	public synchronized Duration remainingOpen() {
		if (state != State.OPEN) {
			return Duration.ZERO;
		}
		Duration left = Duration.between(clock.instant(), openUntil);
		return left.isNegative() ? Duration.ZERO : left;
	}

	private synchronized void onSuccess(Permit permit) {
		if (permit.epoch != epoch) {
			return;
		}
		if (state == State.CLOSED) {
			consecutiveFailures = 0;
		}
		else if (state == State.HALF_OPEN) {
			currentOpenFor = openFor;
			consecutiveFailures = 0;
			moveTo(State.CLOSED);
			log.info("circuit closed: the probe succeeded");
		}
	}

	private synchronized void onFailure(Permit permit) {
		if (permit.epoch == epoch) {
			if (state == State.CLOSED) {
				if (++consecutiveFailures >= failureThreshold) {
					trip(openFor);
				}
			}
			else if (state == State.HALF_OPEN) {
				trip(currentOpenFor.multipliedBy(2).compareTo(maxOpenFor) > 0 ? maxOpenFor
						: currentOpenFor.multipliedBy(2));
			}
		}
	}

	private synchronized void onCancel(Permit permit) {
		if (permit.probe && permit.epoch == epoch && state == State.HALF_OPEN) {
			probeInFlight = false;
		}
	}

	private void trip(Duration openPeriod) {
		currentOpenFor = openPeriod;
		openUntil = clock.instant().plus(openPeriod);
		trips++;
		moveTo(State.OPEN);
		log.warn("circuit OPEN for {}: the downstream looks unavailable", openPeriod);
	}

	private void moveTo(State next) {
		state = next;
		epoch++;
		probeInFlight = false;
	}

	/** One call attempt. Complete it exactly once; further calls are ignored. */
	public final class Permit {
		private final long epoch;
		private final boolean probe;
		private final AtomicBoolean done = new AtomicBoolean();

		private Permit(long epoch, boolean probe) {
			this.epoch = epoch;
			this.probe = probe;
		}

		/** The downstream answered sensibly, even if the answer was "no" or "slow down". */
		public void success() {
			if (done.compareAndSet(false, true)) {
				onSuccess(this);
			}
		}

		/** The downstream looks unhealthy (timeout, 5xx). */
		public void failure() {
			if (done.compareAndSet(false, true)) {
				onFailure(this);
			}
		}

		/** No call was made. */
		public void cancel() {
			if (done.compareAndSet(false, true)) {
				onCancel(this);
			}
		}
	}
}
