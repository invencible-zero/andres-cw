package com.invencible.labs.cw.fanout.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class BackoffTest {

	private final Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(30),
			new SplittableRandom(7));

	@Test
	void growsExponentiallyBetweenHalfAndFullOfTheCeiling() {
		for (int failures = 0; failures < 5; failures++) {
			long ceiling = 1000L << failures;
			for (int sample = 0; sample < 200; sample++) {
				assertThat(backoff.delay(failures).toMillis()).isBetween(ceiling / 2, ceiling);
			}
		}
	}

	@Test
	void isCappedEvenForAbsurdFailureCounts() {
		for (int failures : new int[] { 10, 50, 1000, Integer.MAX_VALUE }) {
			assertThat(backoff.delay(failures).toMillis()).isBetween(15_000L, 30_000L);
		}
	}

	@Test
	void jitterSpreadsAWaveSoItDoesNotRetryInLockstep() {
		Set<Long> distinct = IntStream.range(0, 100).mapToObj(i -> backoff.delay(3).toMillis())
				.collect(Collectors.toSet());

		assertThat(distinct.size()).isGreaterThan(50);
	}
}
