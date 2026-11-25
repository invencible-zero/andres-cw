package com.invencible.labs.cw.fanout.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.invencible.labs.cw.fanout.engine.FanOutStats.Counter;
import com.invencible.labs.cw.fanout.model.LoadException;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex.Status;

/**
 * The worker against a simulated downstream, with real threads. The assertions are invariants (never over the limit,
 * never twice, never lost), not timings, so they hold on a slow machine too.
 */
class LoadSchedulerTest {

	@Test
	void drainsEveryTaskWithoutEverExceedingTheConcurrencyLimit() throws Exception {
		try (SchedulerRig rig = new SchedulerRig(s -> {
			s.maxConcurrency = 6;
			s.initialConcurrency = 6;
		})) {
			List<Long> files = new ArrayList<>(rig.unverifiedFiles(1, 40));
			files.addAll(rig.unverifiedFiles(2, 5));
			files.addAll(rig.unverifiedFiles(3, 5));
			rig.worker.handle(rig.event());

			assertThat(rig.drain()).isTrue();

			assertThat(files).allSatisfy(id -> assertThat(rig.index.statusOf(id)).isEqualTo(Status.UPDATES_PENDING));
			assertThat(rig.loader.peakConcurrency()).as("what the downstream really saw").isLessThanOrEqualTo(6);
			assertThat(rig.stats.peakInFlight()).isLessThanOrEqualTo(6);
			assertThat(rig.stats.get(Counter.LOADS_OK)).isEqualTo(50);
			assertThat(rig.loader.totalCalls()).as("not one wasted call").isEqualTo(50);
			assertThat(rig.store.deadLetters()).isEmpty();
		}
	}

	@Test
	void aReplayedEventNeverLoadsAnythingTwice() throws Exception {
		try (SchedulerRig rig = new SchedulerRig()) {
			rig.unverifiedFiles(1, 30);

			rig.worker.handle(rig.event());
			rig.worker.handle(rig.event());
			assertThat(rig.drain()).isTrue();
			rig.worker.handle(rig.event());
			assertThat(rig.scheduler.awaitIdle(Duration.ofSeconds(5))).isTrue();

			assertThat(rig.loader.totalCalls()).isEqualTo(30);
			assertThat(rig.loader.filesLoadedMoreThanOnce()).isEmpty();
			assertThat(rig.stats.get(Counter.TASKS_ENQUEUED)).isEqualTo(30);
			assertThat(rig.stats.get(Counter.TASKS_DUPLICATE)).as("the second delivery, before the drain").isEqualTo(30);
		}
	}

	@Test
	void aSmallFirmIsNotStarvedByTheGiantOne() throws Exception {
		try (SchedulerRig rig = new SchedulerRig(s -> {
			s.maxConcurrency = 3;
			s.initialConcurrency = 3;
		})) {
			rig.unverifiedFiles(1, 150);
			List<Long> small = rig.unverifiedFiles(2, 5);
			rig.worker.handle(rig.event());

			assertThat(rig.drain()).isTrue();

			List<Long> order = rig.loader.callOrder();
			int lastSmallFirmCall = small.stream().mapToInt(order::indexOf).max().orElseThrow();
			assertThat(lastSmallFirmCall)
					.as("the small firm was queued behind 150 tasks but is served within its first turns")
					.isLessThan(30);
		}
	}

	@Test
	void transientFailuresAreRetriedAndAPermanentOneIsDeadLetteredAtOnce() throws Exception {
		try (SchedulerRig rig = new SchedulerRig(s -> s.maxAttempts = 5)) {
			List<Long> files = rig.unverifiedFiles(1, 3);
			long flaky = files.get(0);
			long corrupt = files.get(1);
			long hopeless = files.get(2);
			rig.loader.script(flaky, LoadException.unavailable("503", null), LoadException.unavailable("503", null));
			rig.loader.poison(corrupt);
			rig.loader.script(hopeless, IntStream.range(0, 50).mapToObj(i -> LoadException.timeout("hangs"))
					.toArray(LoadException[]::new));
			rig.worker.handle(rig.event());

			assertThat(rig.drain()).isTrue();

			assertThat(rig.index.statusOf(flaky)).as("recovered by retrying").isEqualTo(Status.UPDATES_PENDING);
			assertThat(rig.loader.callsTo(flaky)).isEqualTo(3);
			assertThat(rig.loader.callsTo(corrupt)).as("a permanent failure is not retried").isEqualTo(1);
			assertThat(rig.loader.callsTo(hopeless)).as("it ran out of attempts").isGreaterThanOrEqualTo(5);
			assertThat(rig.store.deadLetters()).extracting(d -> d.task().fileId())
					.containsExactlyInAnyOrder(corrupt, hopeless);
			assertThat(rig.index.statusOf(corrupt)).as("never shown as up to date").isEqualTo(Status.UNKNOWN);
			assertThat(rig.index.statusOf(hopeless)).isEqualTo(Status.UNKNOWN);
		}
	}

	@Test
	void throttlingSlowsTheWorkerDownWithoutSpendingAnyTaskAttempts() throws Exception {
		try (SchedulerRig rig = new SchedulerRig(s -> {
			s.downstreamCapacity = 3;
			s.initialConcurrency = 10;
			s.maxConcurrency = 10;
			s.maxAttempts = 2;
		})) {
			List<Long> files = rig.unverifiedFiles(1, 60);
			rig.worker.handle(rig.event());

			assertThat(rig.drain()).isTrue();

			assertThat(rig.stats.get(Counter.LOADS_THROTTLED)).as("the downstream said 429").isPositive();
			assertThat(rig.limiter.decreases()).as("and the limiter listened").isPositive();
			assertThat(rig.loader.peakConcurrency()).isLessThanOrEqualTo(3);
			assertThat(rig.store.deadLetters()).as("a 429 is not the task's fault, even with only 2 attempts").isEmpty();
			assertThat(files).allSatisfy(id -> assertThat(rig.index.statusOf(id)).isEqualTo(Status.UPDATES_PENDING));
		}
	}

	@Test
	void theBreakerStopsHammeringADeadDownstreamAndTheQueueSurvivesTheOutage() throws Exception {
		try (SchedulerRig rig = new SchedulerRig()) {
			List<Long> files = rig.unverifiedFiles(1, 40);
			rig.loader.setOutage(true);
			rig.worker.handle(rig.event());
			rig.scheduler.start();

			Thread.sleep(600);
			assertThat(rig.breaker.trips()).isPositive();
			assertThat(rig.loader.callsDuringOutage())
					.as("one wave in flight, then only probes, instead of 40 tasks x 5 attempts")
					.isLessThan(25);

			rig.loader.setOutage(false);
			assertThat(rig.scheduler.awaitIdle(Duration.ofSeconds(30))).isTrue();

			assertThat(rig.store.deadLetters()).as("calls stopped while the breaker was open, so no retry budget burned")
					.isEmpty();
			assertThat(files).allSatisfy(id -> assertThat(rig.index.statusOf(id)).isEqualTo(Status.UPDATES_PENDING));
		}
	}

	@Test
	void aFileThatAUserOpenedMeanwhileIsNotLoadedAgain() throws Exception {
		try (SchedulerRig rig = new SchedulerRig()) {
			List<Long> files = rig.unverifiedFiles(1, 10);
			rig.worker.handle(rig.event());

			// four of them are opened by users while their tasks wait: the index heals itself on load
			files.subList(0, 4).forEach(id -> rig.index.markVerified(id, "v1", rig.clock.instant()));
			assertThat(rig.drain()).isTrue();

			assertThat(rig.stats.get(Counter.LOADS_SKIPPED_NOT_NEEDED)).isEqualTo(4);
			assertThat(rig.loader.totalCalls()).as("a minute of someone else's capacity each").isEqualTo(6);
		}
	}

	@Test
	void shuttingDownReturnsTheTasksInFlightToTheQueueInsteadOfLosingThem() throws Exception {
		SchedulerRig rig = new SchedulerRig(s -> {
			s.latency = Duration.ofSeconds(5);
			s.shutdownGrace = Duration.ofMillis(200);
		});
		rig.unverifiedFiles(1, 5);
		rig.worker.handle(rig.event());
		rig.scheduler.start();
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (rig.limiter.inFlight() == 0 && System.nanoTime() < deadline) {
			Thread.sleep(5);
		}
		assertThat(rig.limiter.inFlight()).isPositive();

		rig.close();

		assertThat(rig.store.counts().inFlight()).isZero();
		assertThat(rig.store.counts().queued()).as("nothing lost, nothing counted as a failure").isEqualTo(5);
		assertThat(rig.store.deadLetters()).isEmpty();
		assertThat(rig.limiter.inFlight()).as("every permit came back").isZero();
	}
}
