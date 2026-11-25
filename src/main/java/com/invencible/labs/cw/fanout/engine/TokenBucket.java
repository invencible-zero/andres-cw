package com.invencible.labs.cw.fanout.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Global cap on the rate at which calls <em>start</em>. Concurrency alone allows a burst of {@code limit} calls in the
 * same millisecond, and again one call-duration later when they all finish together. The bucket spreads those starts
 * out, and it is what absorbs the restart after a pause or an outage.
 *
 * <p>The downstream's {@code Retry-After} pauses the whole bucket: a 429 for one call is information for all of them.
 */
public final class TokenBucket {

	private final double ratePerSecond;
	private final double burst;
	private final Clock clock;

	private double tokens;
	private Instant lastRefill;
	private Instant pausedUntil;

	public TokenBucket(double ratePerSecond, int burst, Clock clock) {
		if (ratePerSecond <= 0 || burst < 1) {
			throw new IllegalArgumentException("rate must be > 0 and burst >= 1");
		}
		this.ratePerSecond = ratePerSecond;
		this.burst = burst;
		this.clock = clock;
		this.tokens = burst;
		this.lastRefill = clock.instant();
		this.pausedUntil = Instant.MIN;
	}

	public synchronized boolean tryTake() {
		Instant now = clock.instant();
		if (now.isBefore(pausedUntil)) {
			return false;
		}
		refill(now);
		if (tokens < 1.0) {
			return false;
		}
		tokens -= 1.0;
		return true;
	}

	/** Gives a token back when it was taken but no call was made. */
	public synchronized void refund() {
		tokens = Math.min(burst, tokens + 1.0);
	}

	/** No starts before {@code until}. Tokens do not accumulate during the pause, so the restart is not a burst. */
	public synchronized void pauseUntil(Instant until) {
		if (until.isAfter(pausedUntil)) {
			pausedUntil = until;
			tokens = 0;
			lastRefill = until;
		}
	}

	/** How long until a call may start, so the dispatcher can sleep exactly that long. */
	public synchronized Duration waitTime() {
		Instant now = clock.instant();
		if (now.isBefore(pausedUntil)) {
			return Duration.between(now, pausedUntil);
		}
		refill(now);
		if (tokens >= 1.0) {
			return Duration.ZERO;
		}
		return Duration.ofNanos((long) Math.ceil((1.0 - tokens) / ratePerSecond * 1e9));
	}

	private void refill(Instant now) {
		long elapsedNanos = Math.max(0, Duration.between(lastRefill, now).toNanos());
		tokens = Math.min(burst, tokens + elapsedNanos / 1e9 * ratePerSecond);
		lastRefill = now;
	}
}
