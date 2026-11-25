package com.invencible.labs.cw.fanout.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.invencible.labs.cw.fanout.MutableClock;

class AimdLimiterTest {

	private final MutableClock clock = new MutableClock();

	private AimdLimiter limiter(int min, int initial, int max) {
		return new AimdLimiter(min, initial, max, 0.5, 0.9, Duration.ofSeconds(5), clock);
	}

	@Test
	void neverAdmitsMoreThanTheLimit() {
		AimdLimiter limiter = limiter(1, 3, 10);

		assertThat(limiter.tryAcquire()).isPresent();
		assertThat(limiter.tryAcquire()).isPresent();
		assertThat(limiter.tryAcquire()).isPresent();
		assertThat(limiter.tryAcquire()).isEmpty();
		assertThat(limiter.inFlight()).isEqualTo(3);
	}

	@Test
	void growsWhileTheLimitIsBeingUsedButNeverBeyondTheMaximum() {
		AimdLimiter limiter = limiter(1, 4, 8);

		for (int round = 0; round < 200; round++) {
			List<AimdLimiter.Permit> held = new ArrayList<>();
			while (limiter.tryAcquire().map(held::add).isPresent()) {
				// fill the limit
			}
			held.forEach(AimdLimiter.Permit::success);
		}

		assertThat(limiter.limit()).isEqualTo(8);
	}

	@Test
	void doesNotDriftUpwardWhileIdle() {
		AimdLimiter limiter = limiter(1, 10, 100);

		for (int i = 0; i < 1000; i++) {
			limiter.tryAcquire().orElseThrow().success();
		}

		assertThat(limiter.limit()).isEqualTo(10);
	}

	@Test
	void aThrottleHalvesTheLimitButATimeoutOnlyTrimsIt() {
		AimdLimiter throttled = limiter(1, 20, 100);
		throttled.tryAcquire().orElseThrow().throttled();
		assertThat(throttled.limit()).isEqualTo(10);

		AimdLimiter timedOut = limiter(1, 20, 100);
		timedOut.tryAcquire().orElseThrow().timedOut();
		assertThat(timedOut.limit()).isEqualTo(18);
	}

	@Test
	void aBurstOfSignalsCountsOncePerCooldown() {
		AimdLimiter limiter = limiter(1, 64, 100);
		List<AimdLimiter.Permit> wave = new ArrayList<>();
		for (int i = 0; i < 7; i++) {
			wave.add(limiter.tryAcquire().orElseThrow());
		}

		wave.forEach(AimdLimiter.Permit::throttled);
		assertThat(limiter.limit()).as("seven simultaneous 429s are one event").isEqualTo(32);

		clock.advance(Duration.ofSeconds(6));
		limiter.tryAcquire().orElseThrow().throttled();
		assertThat(limiter.limit()).as("a new event after the cooldown counts again").isEqualTo(16);
		assertThat(limiter.decreases()).isEqualTo(2);
	}

	@Test
	void neverDropsBelowTheMinimum() {
		AimdLimiter limiter = limiter(3, 4, 10);

		for (int i = 0; i < 10; i++) {
			limiter.tryAcquire().orElseThrow().throttled();
			clock.advance(Duration.ofSeconds(10));
		}

		assertThat(limiter.limit()).isEqualTo(3);
	}

	@Test
	void aPermitIsReleasedOnlyOnce() {
		AimdLimiter limiter = limiter(1, 4, 10);
		AimdLimiter.Permit permit = limiter.tryAcquire().orElseThrow();

		permit.success();
		permit.throttled();
		permit.neutral();

		assertThat(limiter.inFlight()).isZero();
		assertThat(limiter.limit()).as("the late throttle was ignored").isEqualTo(4);
	}
}
