package com.invencible.labs.cw.fanout.engine;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counters for the signals the SLO section of the ADD asks for. Framework-free on purpose: a Micrometer binding can
 * read {@link #get} later without the engine knowing about it.
 */
public final class FanOutStats {

	public enum Counter {
		EVENTS_HANDLED,
		FILES_SCANNED,
		PENDING_WRITTEN,
		PENDING_SKIPPED,
		UNRESOLVABLE_BASELINE,
		TASKS_ENQUEUED,
		TASKS_DUPLICATE,
		LOADS_OK,
		LOADS_SKIPPED_NOT_NEEDED,
		LOADS_THROTTLED,
		LOADS_TIMED_OUT,
		LOADS_UNAVAILABLE,
		LOADS_REJECTED,
		RETRIES,
		DEAD_LETTERED,
		LEASES_EXPIRED,
		POST_LOAD_FAILURES
	}

	private final Map<Counter, LongAdder> counters = new EnumMap<>(Counter.class);
	private final AtomicInteger peakInFlight = new AtomicInteger();

	public FanOutStats() {
		for (Counter c : Counter.values()) {
			counters.put(c, new LongAdder());
		}
	}

	public void inc(Counter counter) {
		counters.get(counter).increment();
	}

	public void add(Counter counter, long n) {
		counters.get(counter).add(n);
	}

	public long get(Counter counter) {
		return counters.get(counter).sum();
	}

	public void observeInFlight(int inFlight) {
		peakInFlight.accumulateAndGet(inFlight, Math::max);
	}

	public int peakInFlight() {
		return peakInFlight.get();
	}
}
