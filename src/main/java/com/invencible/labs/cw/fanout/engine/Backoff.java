package com.invencible.labs.cw.fanout.engine;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with "equal jitter": half of the delay is fixed and half is random. Full jitter can return
 * almost zero, which would let a failing wave retry at once. No jitter makes every failed task retry in lockstep.
 */
public final class Backoff {

	private final Duration base;
	private final Duration cap;
	private final RandomGenerator random;

	public Backoff(Duration base, Duration cap, RandomGenerator random) {
		this.base = base;
		this.cap = cap;
		this.random = random;
	}

	/** @param failures how many failures were counted before this retry (0 for the first retry) */
	public Duration delay(int failures) {
		long exponential = base.toMillis() * (1L << Math.min(failures, 20));
		long ceiling = Math.min(cap.toMillis(), exponential);
		long half = ceiling / 2;
		return Duration.ofMillis(half + (half > 0 ? random.nextLong(half + 1) : 0));
	}
}
