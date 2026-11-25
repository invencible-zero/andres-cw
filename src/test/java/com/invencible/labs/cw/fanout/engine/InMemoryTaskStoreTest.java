package com.invencible.labs.cw.fanout.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.invencible.labs.cw.fanout.MutableClock;
import com.invencible.labs.cw.fanout.model.LoadTask;
import com.invencible.labs.cw.fanout.model.TaskKey;
import com.invencible.labs.cw.fanout.port.TaskStore.Claim;
import com.invencible.labs.cw.fanout.port.TaskStore.Counts;
import com.invencible.labs.cw.fanout.port.TaskStore.EnqueueResult;
import com.invencible.labs.cw.fanout.port.TaskStore.FailResult;

class InMemoryTaskStoreTest {

	private static final Duration LEASE = Duration.ofMinutes(3);

	private final MutableClock clock = new MutableClock();

	private InMemoryTaskStore store(int perFirmCap, int maxAttempts) {
		return new InMemoryTaskStore(perFirmCap, maxAttempts, LEASE, clock);
	}

	private static LoadTask task(long file, long firm, long revision) {
		return new LoadTask(new TaskKey(file, TaskKey.Operation.LOAD_VERIFY, "v2", revision), firm, 1);
	}

	@Test
	void enqueueIsIdempotentInEveryState() {
		InMemoryTaskStore store = store(10, 3);
		LoadTask finished = task(1, 1, 1);
		LoadTask poison = task(2, 1, 1);

		assertThat(store.enqueue(finished)).isEqualTo(EnqueueResult.ENQUEUED);
		assertThat(store.enqueue(finished)).as("queued").isEqualTo(EnqueueResult.DUPLICATE);
		Claim claim = store.claimNext().orElseThrow();
		assertThat(store.enqueue(finished)).as("in flight").isEqualTo(EnqueueResult.DUPLICATE);
		store.complete(claim);
		assertThat(store.enqueue(finished)).as("done").isEqualTo(EnqueueResult.DUPLICATE);

		store.enqueue(poison);
		store.deadLetter(store.claimNext().orElseThrow(), "corrupt");
		assertThat(store.enqueue(poison)).as("dead: a replay must not re-poison the queue")
				.isEqualTo(EnqueueResult.DUPLICATE);
		assertThat(store.counts()).isEqualTo(new Counts(0, 0, 1, 1));
	}

	@Test
	void servesTheFirmWithTheFewestTasksInFlightFirst() {
		InMemoryTaskStore store = store(10, 3);
		for (long file = 1; file <= 5; file++) {
			store.enqueue(task(file, 1, 1));
		}
		store.enqueue(task(100, 2, 1));

		assertThat(store.claimNext().orElseThrow().task().firmId()).as("oldest first when equal").isEqualTo(1);
		assertThat(store.claimNext().orElseThrow().task().firmId())
				.as("the late small firm jumps the giant firm's queue").isEqualTo(2);
		assertThat(store.claimNext().orElseThrow().task().firmId()).isEqualTo(1);
	}

	@Test
	void aFirmThatIsAloneMayUseEverythingUpToItsCap() {
		InMemoryTaskStore store = store(2, 3);
		for (long file = 1; file <= 5; file++) {
			store.enqueue(task(file, 1, 1));
		}

		Claim first = store.claimNext().orElseThrow();
		store.claimNext().orElseThrow();
		assertThat(store.claimNext()).as("the cap protects the firm's own store").isEmpty();

		store.complete(first);
		assertThat(store.claimNext()).isPresent();
	}

	@Test
	void neverRunsTwoTasksForTheSameFileAtOnce() {
		InMemoryTaskStore store = store(10, 3);
		store.enqueue(task(1, 1, 1));
		store.enqueue(task(1, 1, 2));

		Claim first = store.claimNext().orElseThrow();
		assertThat(store.claimNext()).as("another task for the file is in flight").isEmpty();

		store.complete(first);
		assertThat(store.claimNext().orElseThrow().task().key().lineageRevision()).isEqualTo(2);
	}

	@Test
	void aRetryWaitsForItsBackoff() {
		InMemoryTaskStore store = store(10, 3);
		store.enqueue(task(1, 1, 1));
		Claim claim = store.claimNext().orElseThrow();

		assertThat(store.fail(claim, clock.instant().plusSeconds(10), "503")).isEqualTo(FailResult.RETRY);
		assertThat(store.claimNext()).isEmpty();
		clock.advance(Duration.ofSeconds(9));
		assertThat(store.claimNext()).isEmpty();
		clock.advance(Duration.ofSeconds(2));
		assertThat(store.claimNext().orElseThrow().attempt()).as("one failure counted").isEqualTo(1);
	}

	@Test
	void exhaustedAttemptsGoToTheDeadLetterQueueAndCanBeReplayed() {
		InMemoryTaskStore store = store(10, 3);
		LoadTask task = task(1, 1, 1);
		store.enqueue(task);

		assertThat(store.fail(store.claimNext().orElseThrow(), clock.instant(), "timeout")).isEqualTo(FailResult.RETRY);
		assertThat(store.fail(store.claimNext().orElseThrow(), clock.instant(), "timeout")).isEqualTo(FailResult.RETRY);
		assertThat(store.fail(store.claimNext().orElseThrow(), clock.instant(), "timeout")).isEqualTo(FailResult.DEAD);

		assertThat(store.claimNext()).isEmpty();
		assertThat(store.deadLetters()).singleElement().satisfies(dead -> {
			assertThat(dead.task()).isEqualTo(task);
			assertThat(dead.attempts()).isEqualTo(3);
			assertThat(dead.reason()).isEqualTo("timeout");
		});

		assertThat(store.requeueDead(task.key())).isTrue();
		assertThat(store.requeueDead(task.key())).as("it is not dead any more").isFalse();
		assertThat(store.claimNext().orElseThrow().attempt()).as("a fresh budget").isZero();
		assertThat(store.deadLetters()).isEmpty();
	}

	@Test
	void deferringDoesNotSpendAnAttempt() {
		InMemoryTaskStore store = store(10, 2);
		store.enqueue(task(1, 1, 1));

		for (int throttles = 0; throttles < 10; throttles++) {
			Claim claim = store.claimNext().orElseThrow();
			assertThat(claim.attempt()).isZero();
			assertThat(store.defer(claim, clock.instant())).isTrue();
		}

		assertThat(store.fail(store.claimNext().orElseThrow(), clock.instant(), "x")).isEqualTo(FailResult.RETRY);
	}

	@Test
	void aLostLeaseReturnsTheTaskAndFencesTheOldWorker() {
		InMemoryTaskStore store = store(10, 3);
		store.enqueue(task(1, 1, 1));
		Claim old = store.claimNext().orElseThrow();

		assertThat(store.reclaimExpired()).as("the lease is still valid").isZero();
		clock.advance(LEASE.plusSeconds(1));
		assertThat(store.reclaimExpired()).isEqualTo(1);

		Claim fresh = store.claimNext().orElseThrow();
		assertThat(fresh.attempt()).as("a crash counts as an attempt").isEqualTo(1);
		assertThat(store.complete(old)).as("the old worker lost its lease").isFalse();
		assertThat(store.fail(old, clock.instant(), "late")).isEqualTo(FailResult.STALE);
		assertThat(store.defer(old, clock.instant())).isFalse();
		assertThat(store.deadLetter(old, "late")).isFalse();

		assertThat(store.complete(fresh)).isTrue();
		assertThat(store.counts()).isEqualTo(new Counts(0, 0, 1, 0));
	}

	@Test
	void aPoisonTaskThatKeepsKillingWorkersEndsUpDead() {
		InMemoryTaskStore store = store(10, 2);
		store.enqueue(task(1, 1, 1));

		for (int crash = 0; crash < 2; crash++) {
			store.claimNext().orElseThrow();
			clock.advance(LEASE.plusSeconds(1));
			store.reclaimExpired();
		}

		assertThat(store.claimNext()).isEmpty();
		assertThat(store.deadLetters()).singleElement().satisfies(dead -> assertThat(dead.reason())
				.isEqualTo("lease expired"));
	}

	@Test
	void countsFollowTheStateOfTheTasks() {
		InMemoryTaskStore store = store(10, 3);
		for (long file = 1; file <= 4; file++) {
			store.enqueue(task(file, file, 1));
		}
		Claim done = store.claimNext().orElseThrow();
		Claim dead = store.claimNext().orElseThrow();
		store.claimNext().orElseThrow();
		store.complete(done);
		store.deadLetter(dead, "x");

		assertThat(store.counts()).isEqualTo(new Counts(1, 1, 1, 1));
		assertThat(store.counts().idle()).isFalse();
	}
}
