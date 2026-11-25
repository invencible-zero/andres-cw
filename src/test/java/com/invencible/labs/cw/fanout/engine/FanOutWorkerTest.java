package com.invencible.labs.cw.fanout.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.invencible.labs.cw.fanout.MutableClock;
import com.invencible.labs.cw.fanout.engine.FanOutStats.Counter;
import com.invencible.labs.cw.fanout.engine.FanOutWorker.HandleResult;
import com.invencible.labs.cw.fanout.model.EngagementFile;
import com.invencible.labs.cw.fanout.model.LoadResult;
import com.invencible.labs.cw.fanout.model.LoadTask;
import com.invencible.labs.cw.fanout.model.TaskKey;
import com.invencible.labs.cw.fanout.model.TemplateVersionEvent;
import com.invencible.labs.cw.fanout.model.VerificationState;
import com.invencible.labs.cw.fanout.port.EngagementIndex.PendingWrite;
import com.invencible.labs.cw.fanout.port.TaskStore.Counts;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex.Status;
import com.invencible.labs.cw.fanout.sim.InMemoryVersionGraphSource;

class FanOutWorkerTest {

	private static final long TEMPLATE = 1;
	private static final long OTHER_TEMPLATE = 2;

	private final MutableClock clock = new MutableClock();
	private final InMemoryEngagementIndex index = new InMemoryEngagementIndex();
	private final InMemoryVersionGraphSource graphs = new InMemoryVersionGraphSource();
	private final InMemoryTaskStore store = new InMemoryTaskStore(100, 5, Duration.ofMinutes(3), clock);
	private final FanOutStats stats = new FanOutStats();
	private final FanOutWorker worker = new FanOutWorker(graphs, index, store, new FanOutWorker.Config(3, 7), clock,
			stats);
	private long revision;

	@BeforeEach
	void publishV2() {
		graphs.publish(TEMPLATE, "v1", "default");
		revision = graphs.publish(TEMPLATE, "v2", "default", "v1");
	}

	private TemplateVersionEvent event() {
		return new TemplateVersionEvent("evt-" + revision, TemplateVersionEvent.Type.PUBLISHED, 1, TEMPLATE, "v2",
				List.of("v1"), "default", revision, clock.instant());
	}

	private void file(long id, long firm, String baseline, VerificationState state, String... declined) {
		index.add(id, firm, TEMPLATE, baseline, state, Set.of(declined));
	}

	@Test
	void recomputesThePendingUpdatesOfEveryIndexedFileOfTheTemplate() {
		file(1, 1, "v1", VerificationState.VERIFIED);
		file(2, 1, "v2", VerificationState.VERIFIED);
		file(3, 2, "v1", VerificationState.VERIFIED, "v2");
		index.add(4, 3, OTHER_TEMPLATE, "v1", VerificationState.VERIFIED, Set.of());

		HandleResult result = worker.handle(event());

		assertThat(index.statusOf(1)).isEqualTo(Status.UPDATES_PENDING);
		assertThat(index.pendingOf(1)).containsExactly("v2");
		assertThat(index.statusOf(2)).as("already on v2").isEqualTo(Status.UP_TO_DATE);
		assertThat(index.statusOf(3)).as("declined v2: not offered again for the same version")
				.isEqualTo(Status.UP_TO_DATE);
		assertThat(index.statusOf(4)).as("another template is not touched").isEqualTo(Status.UNKNOWN);
		assertThat(result.filesScanned()).isEqualTo(3);
		assertThat(result.pendingWritten()).isEqualTo(3);
		assertThat(result.tasksEnqueued()).isZero();
	}

	@Test
	void unverifiedFilesAreNeverShownAsUpToDateAndGetALoadTaskInstead() {
		file(1, 1, "v1", VerificationState.UNKNOWN);
		file(2, 7, "v2", VerificationState.UNKNOWN);

		HandleResult result = worker.handle(event());

		assertThat(index.statusOf(1)).isEqualTo(Status.UNKNOWN);
		assertThat(index.statusOf(2)).as("even though its best-known baseline is the latest").isEqualTo(Status.UNKNOWN);
		assertThat(result.tasksEnqueued()).isEqualTo(2);
		assertThat(store.counts()).isEqualTo(new Counts(2, 0, 0, 0));
	}

	@Test
	void aBaselineTheGraphDoesNotKnowIsDemotedToUnknownAndVerified() {
		file(1, 1, "v-nowhere", VerificationState.VERIFIED);

		HandleResult result = worker.handle(event());

		assertThat(result.unresolvable()).isEqualTo(1);
		assertThat(index.statusOf(1)).isEqualTo(Status.UNKNOWN);
		assertThat(index.get(1).orElseThrow().state()).isEqualTo(VerificationState.UNKNOWN);
		assertThat(result.tasksEnqueued()).isEqualTo(1);
	}

	@Test
	void deliveringTheSameEventAgainChangesNothing() {
		file(1, 1, "v1", VerificationState.VERIFIED);
		file(2, 1, "v1", VerificationState.VERIFIED);
		file(3, 2, "v1", VerificationState.UNKNOWN);

		HandleResult first = worker.handle(event());
		HandleResult replay = worker.handle(event());

		assertThat(first.pendingWritten()).isEqualTo(2);
		assertThat(first.tasksEnqueued()).isEqualTo(1);
		assertThat(replay.pendingWritten()).as("already computed at this revision").isZero();
		assertThat(replay.pendingSkipped()).isEqualTo(2);
		assertThat(replay.tasksEnqueued()).isZero();
		assertThat(replay.tasksDuplicate()).isEqualTo(1);
		assertThat(store.counts().queued()).isEqualTo(1);
	}

	@Test
	void aNewerRevisionRewritesTheRowsAndAWithdrawalFollowsTheGraph() {
		file(1, 1, "v1", VerificationState.VERIFIED);
		worker.handle(event());
		assertThat(index.pendingOf(1)).containsExactly("v2");

		revision = graphs.withdraw(TEMPLATE, "v2");
		HandleResult withdrawal = worker.handle(new TemplateVersionEvent("evt-w", TemplateVersionEvent.Type.WITHDRAWN,
				1, TEMPLATE, "v2", List.of("v1"), "default", revision, clock.instant()));

		assertThat(withdrawal.pendingWritten()).isEqualTo(1);
		assertThat(index.pendingOf(1)).isEmpty();
		assertThat(index.statusOf(1)).isEqualTo(Status.UP_TO_DATE);
	}

	@Test
	void scansEveryFileExactlyOnceAcrossShardsAndPages() {
		for (long id = 1; id <= 100; id++) {
			file(id, id % 5, "v1", VerificationState.VERIFIED);
		}

		HandleResult result = worker.handle(event());

		assertThat(result.filesScanned()).as("3 shards, pages of 7").isEqualTo(100);
		assertThat(result.pendingWritten()).isEqualTo(100);
		assertThat(stats.get(Counter.FILES_SCANNED)).isEqualTo(100);
	}

	@Test
	void aFailureInTheFastPathSurfacesSoTheEventIsNotAcknowledged() {
		file(1, 1, "v1", VerificationState.VERIFIED);
		VersionGraphSourceThatFails failing = new VersionGraphSourceThatFails();
		FanOutWorker broken = new FanOutWorker(failing, index, store, new FanOutWorker.Config(3, 7), clock, stats);

		assertThatThrownBy(() -> broken.handle(event())).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("product engine is down");
	}

	// ---------------------------------------------------------------- the conditional writes of the index

	@Test
	void aWriteComputedFromStaleInputsIsRefused() {
		file(1, 1, "v1", VerificationState.VERIFIED);
		EngagementFile scanned = index.get(1).orElseThrow();

		index.markUnknown(List.of(1L));
		index.markVerified(1, "v2", clock.instant());

		int applied = index.replacePending(List.of(new PendingWrite(1, TEMPLATE, Set.of("v2"), scanned.rowVersion(), 9,
				clock.instant())));

		assertThat(applied).as("the baseline changed after the scan: the result describes an old file").isZero();
	}

	@Test
	void anOlderRevisionNeverOverwritesANewerOneAndAReplayIsANoOp() {
		file(1, 1, "v1", VerificationState.VERIFIED);
		long rowVersion = index.get(1).orElseThrow().rowVersion();

		assertThat(index.replacePending(List.of(write(rowVersion, 5, "v5")))).isEqualTo(1);
		assertThat(index.replacePending(List.of(write(rowVersion, 4, "v4")))).as("older").isZero();
		assertThat(index.replacePending(List.of(write(rowVersion, 5, "v5")))).as("replay").isZero();
		assertThat(index.replacePending(List.of(write(rowVersion, 6, "v6")))).as("newer").isEqualTo(1);

		assertThat(index.pendingOf(1)).containsExactly("v6");
	}

	private PendingWrite write(long rowVersion, long revision, String target) {
		return new PendingWrite(1, TEMPLATE, Set.of(target), rowVersion, revision, clock.instant());
	}

	// ---------------------------------------------------------------- the self-healing path (after a load)

	private LoadTask loadTask(long fileId, long firm) {
		return new LoadTask(new TaskKey(fileId, TaskKey.Operation.LOAD_VERIFY, "v2", revision), firm, TEMPLATE);
	}

	@Test
	void aSuccessfulLoadVerifiesTheFileAndRecomputesItAgainstTheCurrentGraph() {
		file(1, 1, "v1", VerificationState.UNKNOWN);
		worker.handle(event());
		assertThat(index.statusOf(1)).isEqualTo(Status.UNKNOWN);

		// the load reveals that the file had in fact already moved to v2
		worker.onLoaded(loadTask(1, 1), new LoadResult(1, "v2", clock.instant()));

		assertThat(index.get(1).orElseThrow().baselineVersion()).isEqualTo("v2");
		assertThat(index.statusOf(1)).isEqualTo(Status.UP_TO_DATE);
	}

	@Test
	void aLoadResultThatArrivesAfterSomethingNewerVerifiedTheFileIsDropped() {
		file(1, 1, "v1", VerificationState.UNKNOWN);
		// a user opened the file and applied v2 while our one-minute load was running
		index.markVerified(1, "v2", clock.instant());

		worker.onLoaded(loadTask(1, 1), new LoadResult(1, "v1", clock.instant()));

		assertThat(index.get(1).orElseThrow().baselineVersion()).as("the newer fact wins").isEqualTo("v2");
	}

	@Test
	void aLoadIsOnlyNeededWhileTheFileIsStillUnverified() {
		file(1, 1, "v1", VerificationState.UNKNOWN);
		LoadTask task = loadTask(1, 1);

		assertThat(worker.isStillNeeded(task)).isTrue();
		index.markVerified(1, "v1", clock.instant());
		assertThat(worker.isStillNeeded(task)).as("a user opened it meanwhile").isFalse();
		assertThat(worker.isStillNeeded(loadTask(99, 1))).as("the file is gone").isFalse();
	}

	private static final class VersionGraphSourceThatFails implements com.invencible.labs.cw.fanout.port.VersionGraphSource {
		@Override
		public com.invencible.labs.cw.fanout.model.VersionGraph load(long templateId) {
			throw new IllegalStateException("product engine is down");
		}
	}
}
