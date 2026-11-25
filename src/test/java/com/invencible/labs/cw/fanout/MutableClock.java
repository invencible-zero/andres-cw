package com.invencible.labs.cw.fanout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** A clock that only moves when the test says so, which makes every timing rule deterministic. */
public final class MutableClock extends Clock {

	private Instant now = Instant.parse("2026-01-01T00:00:00Z");

	public synchronized void advance(Duration duration) {
		now = now.plus(duration);
	}

	@Override
	public ZoneId getZone() {
		return ZoneOffset.UTC;
	}

	@Override
	public Clock withZone(ZoneId zone) {
		return this;
	}

	@Override
	public synchronized Instant instant() {
		return now;
	}
}
