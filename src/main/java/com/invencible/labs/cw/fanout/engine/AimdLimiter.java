package com.invencible.labs.cw.fanout.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adaptive concurrency limit: additive increase, multiplicative decrease (the TCP congestion rule).
 *
 * <p>The downstream's real capacity is unknown to us and moves (other traffic, deploys, slow customers). We start low,
 * grow by one permit per "limit successes" while we are actually using the limit, and shrink on overload signals.
 * The configured maximum is the share agreed with the owning team, not a target.
 *
 * <p>The two overload signals are not equal. A 429 is the downstream saying "too much", so the limit is cut hard
 * (halved). A timeout is ambiguous (one bad file or pod looks the same), so it only trims the limit (by 10%).
 * Treating both as "halve" lets a steady trickle of unrelated timeouts pin the limit far below real capacity.
 *
 * <p>Two more details matter more than the formula. First, one burst of signals (a whole wave of in-flight calls
 * timing out together) counts once per cooldown. Otherwise seven simultaneous timeouts would shrink the limit by a
 * factor of 128. Second, it only grows when {@code inFlight} is near the limit, otherwise an idle worker would drift
 * to the maximum and then slam the downstream when work arrives.
 */
public final class AimdLimiter {

	private enum Signal { SUCCESS, THROTTLED, TIMED_OUT, NEUTRAL }

	private final int minLimit;
	private final int maxLimit;
	private final double throttleRatio;
	private final double timeoutRatio;
	private final Duration decreaseCooldown;
	private final Clock clock;

	private double limit;
	private int inFlight;
	private Instant lastDecrease;
	private long decreases;

	/**
	 * @param throttleRatio factor applied to the limit on a 429, in (0, 1)
	 * @param timeoutRatio  factor applied on a timeout, in (0, 1). Closer to 1 than {@code throttleRatio}
	 */
	public AimdLimiter(int minLimit, int initialLimit, int maxLimit, double throttleRatio, double timeoutRatio,
			Duration decreaseCooldown, Clock clock) {
		if (minLimit < 1 || initialLimit < minLimit || maxLimit < initialLimit) {
			throw new IllegalArgumentException("need 1 <= min <= initial <= max");
		}
		if (throttleRatio <= 0 || throttleRatio >= 1 || timeoutRatio <= 0 || timeoutRatio >= 1) {
			throw new IllegalArgumentException("ratios must be in (0, 1)");
		}
		this.minLimit = minLimit;
		this.maxLimit = maxLimit;
		this.limit = initialLimit;
		this.throttleRatio = throttleRatio;
		this.timeoutRatio = timeoutRatio;
		this.decreaseCooldown = decreaseCooldown;
		this.clock = clock;
	}

	/** Takes a slot if one is free. The permit must be completed exactly once (further calls are ignored). */
	public synchronized Optional<Permit> tryAcquire() {
		if (inFlight >= (int) limit) {
			return Optional.empty();
		}
		inFlight++;
		return Optional.of(new Permit());
	}

	public synchronized int limit() {
		return (int) limit;
	}

	public synchronized int inFlight() {
		return inFlight;
	}

	public synchronized long decreases() {
		return decreases;
	}

	public int maxLimit() {
		return maxLimit;
	}

	private synchronized void release(Signal signal) {
		boolean usingTheLimit = inFlight * 2 >= limit;
		inFlight--;
		switch (signal) {
			case SUCCESS -> {
				if (usingTheLimit) {
					limit = Math.min(maxLimit, limit + 1.0 / limit);
				}
			}
			case THROTTLED -> decrease(throttleRatio);
			case TIMED_OUT -> decrease(timeoutRatio);
			case NEUTRAL -> {
			}
		}
	}

	private void decrease(double ratio) {
		Instant now = clock.instant();
		if (lastDecrease == null || Duration.between(lastDecrease, now).compareTo(decreaseCooldown) >= 0) {
			limit = Math.max(minLimit, limit * ratio);
			lastDecrease = now;
			decreases++;
		}
	}

	/** One unit of the limit. */
	public final class Permit {
		private final AtomicBoolean done = new AtomicBoolean();

		/** The call worked: the limit may grow. */
		public void success() {
			complete(Signal.SUCCESS);
		}

		/** The downstream said 429: the limit is cut hard. */
		public void throttled() {
			complete(Signal.THROTTLED);
		}

		/** The call timed out: ambiguous, so the limit is only trimmed. */
		public void timedOut() {
			complete(Signal.TIMED_OUT);
		}

		/** Releases the slot without saying anything about capacity (also for a permit that was never used). */
		public void neutral() {
			complete(Signal.NEUTRAL);
		}

		private void complete(Signal signal) {
			if (done.compareAndSet(false, true)) {
				release(signal);
			}
		}
	}
}
