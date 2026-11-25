package com.invencible.labs.cw.fanout.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.invencible.labs.cw.fanout.MutableClock;
import com.invencible.labs.cw.fanout.engine.CircuitBreaker.State;

class CircuitBreakerTest {

	private final MutableClock clock = new MutableClock();
	private final CircuitBreaker breaker = new CircuitBreaker(3, Duration.ofSeconds(10), Duration.ofSeconds(40), clock);

	private void failTimes(int n) {
		for (int i = 0; i < n; i++) {
			breaker.tryAcquire().orElseThrow().failure();
		}
	}

	@Test
	void opensAfterConsecutiveFailuresButASuccessInBetweenResetsTheCount() {
		failTimes(2);
		breaker.tryAcquire().orElseThrow().success();
		failTimes(2);
		assertThat(breaker.state()).isEqualTo(State.CLOSED);

		failTimes(1);
		assertThat(breaker.state()).isEqualTo(State.OPEN);
		assertThat(breaker.tryAcquire()).isEmpty();
		assertThat(breaker.trips()).isEqualTo(1);
	}

	@Test
	void letsExactlyOneProbeThroughAfterTheOpenPeriodAndClosesWhenItSucceeds() {
		failTimes(3);
		clock.advance(Duration.ofSeconds(9));
		assertThat(breaker.tryAcquire()).as("still open").isEmpty();

		clock.advance(Duration.ofSeconds(2));
		CircuitBreaker.Permit probe = breaker.tryAcquire().orElseThrow();
		assertThat(breaker.state()).isEqualTo(State.HALF_OPEN);
		assertThat(breaker.tryAcquire()).as("only one probe at a time").isEmpty();

		probe.success();
		assertThat(breaker.state()).isEqualTo(State.CLOSED);
		assertThat(breaker.tryAcquire()).isPresent();
	}

	@Test
	void aFailedProbeReopensWithADoublingPeriodUpToTheCap() {
		failTimes(3);
		for (long expected : new long[] { 20, 40, 40 }) {
			clock.advance(Duration.ofSeconds(41));
			breaker.tryAcquire().orElseThrow().failure();
			assertThat(breaker.state()).isEqualTo(State.OPEN);
			assertThat(breaker.remainingOpen()).isEqualTo(Duration.ofSeconds(expected));
		}
	}

	@Test
	void aCallThatStartedBeforeTheTripCannotBeMistakenForTheProbe() {
		CircuitBreaker.Permit old = breaker.tryAcquire().orElseThrow();
		failTimes(3);
		clock.advance(Duration.ofSeconds(11));
		CircuitBreaker.Permit probe = breaker.tryAcquire().orElseThrow();

		old.success();
		assertThat(breaker.state()).as("a stale success says nothing about the downstream now")
				.isEqualTo(State.HALF_OPEN);

		probe.success();
		assertThat(breaker.state()).isEqualTo(State.CLOSED);
	}

	@Test
	void aStaleFailureCannotTripItAgain() {
		CircuitBreaker.Permit stale = breaker.tryAcquire().orElseThrow();
		failTimes(3);

		stale.failure();

		assertThat(breaker.trips()).isEqualTo(1);
	}

	@Test
	void aCancelledProbeFreesTheSlot() {
		failTimes(3);
		clock.advance(Duration.ofSeconds(11));

		breaker.tryAcquire().orElseThrow().cancel();

		assertThat(breaker.tryAcquire()).isPresent();
	}

	@Test
	void aPermitCountsOnlyOnce() {
		CircuitBreaker.Permit permit = breaker.tryAcquire().orElseThrow();

		permit.failure();
		permit.failure();
		permit.failure();

		assertThat(breaker.state()).isEqualTo(State.CLOSED);
	}
}
