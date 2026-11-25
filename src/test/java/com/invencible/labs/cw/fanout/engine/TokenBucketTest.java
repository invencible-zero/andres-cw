package com.invencible.labs.cw.fanout.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.invencible.labs.cw.fanout.MutableClock;

class TokenBucketTest {

	private final MutableClock clock = new MutableClock();

	@Test
	void allowsABurstThenTheConfiguredRate() {
		TokenBucket bucket = new TokenBucket(2.0, 3, clock);

		assertThat(bucket.tryTake()).isTrue();
		assertThat(bucket.tryTake()).isTrue();
		assertThat(bucket.tryTake()).isTrue();
		assertThat(bucket.tryTake()).as("the burst is spent").isFalse();

		clock.advance(Duration.ofMillis(500));
		assertThat(bucket.tryTake()).as("one token per half second").isTrue();
		assertThat(bucket.tryTake()).isFalse();
	}

	@Test
	void neverAccumulatesBeyondTheBurst() {
		TokenBucket bucket = new TokenBucket(10.0, 2, clock);

		clock.advance(Duration.ofHours(1));

		assertThat(bucket.tryTake()).isTrue();
		assertThat(bucket.tryTake()).isTrue();
		assertThat(bucket.tryTake()).isFalse();
	}

	@Test
	void aRefundedTokenCanBeTakenAgain() {
		TokenBucket bucket = new TokenBucket(1.0, 1, clock);

		assertThat(bucket.tryTake()).isTrue();
		bucket.refund();

		assertThat(bucket.tryTake()).isTrue();
	}

	@Test
	void aRetryAfterPausesEverythingAndTheRestartIsNotABurst() {
		TokenBucket bucket = new TokenBucket(1.0, 5, clock);

		bucket.pauseUntil(clock.instant().plusSeconds(30));
		clock.advance(Duration.ofSeconds(10));
		assertThat(bucket.tryTake()).as("paused").isFalse();
		assertThat(bucket.waitTime()).isEqualTo(Duration.ofSeconds(20));

		clock.advance(Duration.ofSeconds(20));
		assertThat(bucket.tryTake()).as("the pause ended, but no tokens piled up meanwhile").isFalse();
		clock.advance(Duration.ofSeconds(1));
		assertThat(bucket.tryTake()).isTrue();
		assertThat(bucket.tryTake()).isFalse();
	}

	@Test
	void tellsTheDispatcherHowLongToSleep() {
		TokenBucket bucket = new TokenBucket(4.0, 1, clock);
		bucket.tryTake();

		assertThat(bucket.waitTime()).isEqualTo(Duration.ofMillis(250));
		clock.advance(Duration.ofMillis(250));
		assertThat(bucket.waitTime()).isZero();
	}
}
