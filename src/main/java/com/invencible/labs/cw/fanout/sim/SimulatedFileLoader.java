package com.invencible.labs.cw.fanout.sim;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.invencible.labs.cw.fanout.model.LoadException;
import com.invencible.labs.cw.fanout.model.LoadRequest;
import com.invencible.labs.cw.fanout.model.LoadResult;
import com.invencible.labs.cw.fanout.port.FileLoader;

/**
 * The other team's loading service, simulated: it is slow, it has a hard capacity and says 429 beyond it, it
 * sometimes hangs until the caller's deadline, and it can go down. It also keeps the books the demo and the tests
 * check: how many calls each file got and the highest concurrency it ever accepted.
 */
public final class SimulatedFileLoader implements FileLoader {

	/**
	 * @param latency         how long a healthy load takes (the real one is ~1 minute, the demo compresses time)
	 * @param capacity        calls it can serve at once. Beyond that it rejects with a 429
	 * @param retryAfter      the Retry-After it sends with a 429
	 * @param timeoutRate     share of calls that hang until the caller's deadline
	 * @param unavailableRate share of calls that fail with a 5xx
	 */
	public record Behaviour(Duration latency, int capacity, Duration retryAfter, double timeoutRate,
			double unavailableRate) {
	}

	private final Behaviour behaviour;
	private final Clock clock;
	private final Semaphore capacity;
	private final Map<Long, AtomicInteger> callsPerFile = new ConcurrentHashMap<>();
	private final Map<Long, AtomicInteger> successesPerFile = new ConcurrentHashMap<>();
	private final Map<Long, Queue<LoadException>> scripted = new ConcurrentHashMap<>();
	private final Map<Long, String> actualBaselines = new ConcurrentHashMap<>();
	private final Set<Long> poison = ConcurrentHashMap.newKeySet();
	private final Queue<Long> callOrder = new ConcurrentLinkedQueue<>();
	private final AtomicInteger peakConcurrency = new AtomicInteger();
	private final AtomicLong totalCalls = new AtomicLong();
	private final AtomicLong rejectedForCapacity = new AtomicLong();
	private final AtomicLong callsDuringOutage = new AtomicLong();
	private volatile boolean outage;

	public SimulatedFileLoader(Behaviour behaviour, Clock clock) {
		this.behaviour = behaviour;
		this.clock = clock;
		this.capacity = new Semaphore(behaviour.capacity());
	}

	@Override
	public LoadResult load(LoadRequest request) throws InterruptedException {
		long fileId = request.fileId();
		callsPerFile.computeIfAbsent(fileId, id -> new AtomicInteger()).incrementAndGet();
		callOrder.add(fileId);
		totalCalls.incrementAndGet();

		if (outage) {
			callsDuringOutage.incrementAndGet();
			Thread.sleep(5);
			throw LoadException.unavailable("simulated outage", null);
		}
		Queue<LoadException> script = scripted.get(fileId);
		LoadException scriptedFailure = script == null ? null : script.poll();
		if (scriptedFailure != null) {
			Thread.sleep(behaviour.latency().toMillis() / 4);
			throw scriptedFailure;
		}
		if (!capacity.tryAcquire()) {
			rejectedForCapacity.incrementAndGet();
			throw LoadException.throttled(behaviour.retryAfter());
		}
		try {
			peakConcurrency.accumulateAndGet(behaviour.capacity() - capacity.availablePermits(), Math::max);
			ThreadLocalRandom random = ThreadLocalRandom.current();
			if (random.nextDouble() < behaviour.timeoutRate()) {
				Thread.sleep(request.deadline().toMillis());
				throw LoadException.timeout("no answer within " + request.deadline());
			}
			Thread.sleep((long) (behaviour.latency().toMillis() * (0.75 + 0.5 * random.nextDouble())));
			if (poison.contains(fileId)) {
				throw LoadException.rejected("archive is corrupt");
			}
			if (random.nextDouble() < behaviour.unavailableRate()) {
				throw LoadException.unavailable("503 from a loader pod", null);
			}
			successesPerFile.computeIfAbsent(fileId, id -> new AtomicInteger()).incrementAndGet();
			return new LoadResult(fileId, actualBaselines.getOrDefault(fileId, "v1"), clock.instant());
		}
		finally {
			capacity.release();
		}
	}

	// ---------------------------------------------------------------- scenario controls

	public void setOutage(boolean down) {
		this.outage = down;
	}

	/** The next calls for this file fail with these exceptions, in order, before it behaves normally. */
	public void script(long fileId, LoadException... failures) {
		scripted.computeIfAbsent(fileId, id -> new ConcurrentLinkedQueue<>()).addAll(List.of(failures));
	}

	/** Files that can never be loaded (permanent failure). */
	public void poison(long... fileIds) {
		for (long id : fileIds) {
			poison.add(id);
		}
	}

	/** What a load of this file reveals. Defaults to "v1". */
	public void setActualBaseline(long fileId, String version) {
		actualBaselines.put(fileId, version);
	}

	// ---------------------------------------------------------------- the books

	public int callsTo(long fileId) {
		AtomicInteger calls = callsPerFile.get(fileId);
		return calls == null ? 0 : calls.get();
	}

	public Map<Long, Integer> callCounts() {
		return callsPerFile.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
	}

	/** Files that were loaded successfully more than once: capacity spent for nothing. Should stay empty. */
	public List<Long> filesLoadedMoreThanOnce() {
		return successesPerFile.entrySet().stream().filter(e -> e.getValue().get() > 1).map(Map.Entry::getKey)
				.sorted().toList();
	}

	/** File ids in the order the calls arrived. */
	public List<Long> callOrder() {
		return List.copyOf(callOrder);
	}

	/** The highest number of loads this service ever ran at once. */
	public int peakConcurrency() {
		return peakConcurrency.get();
	}

	public long totalCalls() {
		return totalCalls.get();
	}

	public long rejectedForCapacity() {
		return rejectedForCapacity.get();
	}

	public long callsDuringOutage() {
		return callsDuringOutage.get();
	}
}
