package com.invencible.labs.cw.fanout.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.invencible.labs.cw.fanout.engine.FanOutStats.Counter;
import com.invencible.labs.cw.fanout.model.EngagementFile;
import com.invencible.labs.cw.fanout.model.LoadResult;
import com.invencible.labs.cw.fanout.model.LoadTask;
import com.invencible.labs.cw.fanout.model.PendingUpdateCalculator;
import com.invencible.labs.cw.fanout.model.TaskKey;
import com.invencible.labs.cw.fanout.model.TemplateVersionEvent;
import com.invencible.labs.cw.fanout.model.VerificationState;
import com.invencible.labs.cw.fanout.model.VersionGraph;
import com.invencible.labs.cw.fanout.port.EngagementIndex;
import com.invencible.labs.cw.fanout.port.EngagementIndex.PendingWrite;
import com.invencible.labs.cw.fanout.port.TaskStore;
import com.invencible.labs.cw.fanout.port.VersionGraphSource;

/**
 * The template-publish fan-out (ADD section 5). For one {@code TemplateVersionPublished} or
 * {@code TemplateVersionWithdrawn} event it does two very different kinds of work:
 *
 * <ul>
 *   <li><b>Fast path (seconds, no downstream).</b> Reads the current version graph, walks every indexed file of the
 *       template in parallel shards, recomputes each file's pending updates and writes them with conditional writes.
 *       Nothing here calls the slow load.</li>
 *   <li><b>Slow path (hours, protected downstream).</b> A file that cannot be evaluated (UNKNOWN, or a baseline the graph
 *       does not know) is marked UNKNOWN and gets a verification-load task. Tasks are only <em>enqueued</em> here. The
 *       {@link LoadScheduler} spends the downstream's capacity on them.</li>
 * </ul>
 *
 * <h2>Why this is safe to replay</h2>
 * {@link #handle} is idempotent. It never trusts the event's payload, it reads the current graph. Pending rows are a
 * pure function of the inputs, written conditionally. Enqueueing is idempotent per {@link TaskKey}. So a duplicated,
 * late or reordered event, or a crash half-way through, is repaired by delivering the event again. That is also what
 * makes the scan resumable without a checkpoint: the walk is cheap (seconds), the work it would skip is already done.
 *
 * <p>The worker is also the {@link LoadTaskHandler}: when a load succeeds, the same recompute runs for that one file,
 * which is the self-healing path (ADD section 3).
 */
public final class FanOutWorker implements LoadTaskHandler {

	private static final Logger log = LoggerFactory.getLogger(FanOutWorker.class);

	/**
	 * @param shards        parallel slices of the scan, by hash of the file id. They keep the publish-to-indicator time
	 *                      low for the 100,000-file product and spread index writes over many partitions.
	 * @param scanBatchSize rows per index page and per batched write
	 */
	public record Config(int shards, int scanBatchSize) {
	}

	/** What one event did. All counts are over the whole template. */
	public record HandleResult(String eventId, int filesScanned, int pendingWritten, int pendingSkipped,
			int unresolvable, int tasksEnqueued, int tasksDuplicate, Duration elapsed) {
	}

	private record Recomputed(int applied, int skipped, int unresolvable, List<EngagementFile> needsLoad) {
	}

	private record ShardResult(int scanned, int applied, int skipped, int unresolvable, int enqueued,
			int duplicates) {
	}

	private final VersionGraphSource graphs;
	private final EngagementIndex index;
	private final TaskStore store;
	private final Config config;
	private final Clock clock;
	private final FanOutStats stats;

	public FanOutWorker(VersionGraphSource graphs, EngagementIndex index, TaskStore store, Config config, Clock clock,
			FanOutStats stats) {
		this.graphs = graphs;
		this.index = index;
		this.store = store;
		this.config = config;
		this.clock = clock;
		this.stats = stats;
	}

	/**
	 * Handles one event and returns when the fast path is complete and every needed load is enqueued. An exception means
	 * "not done": the consumer must not commit the offset, and redelivery repairs the rest.
	 */
	public HandleResult handle(TemplateVersionEvent event) {
		Instant started = clock.instant();
		VersionGraph graph = graphs.load(event.templateId());
		PendingUpdateCalculator calculator = new PendingUpdateCalculator(graph);

		List<ShardResult> results = new ArrayList<>();
		try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
			List<Future<ShardResult>> futures = IntStream.range(0, config.shards())
					.mapToObj(shard -> pool.submit(() -> processShard(event, calculator, shard)))
					.toList();
			for (Future<ShardResult> future : futures) {
				results.add(await(future));
			}
		}

		HandleResult result = new HandleResult(event.eventId(),
				results.stream().mapToInt(ShardResult::scanned).sum(),
				results.stream().mapToInt(ShardResult::applied).sum(),
				results.stream().mapToInt(ShardResult::skipped).sum(),
				results.stream().mapToInt(ShardResult::unresolvable).sum(),
				results.stream().mapToInt(ShardResult::enqueued).sum(),
				results.stream().mapToInt(ShardResult::duplicates).sum(),
				Duration.between(started, clock.instant()));
		stats.inc(Counter.EVENTS_HANDLED);
		log.info("event {} ({} {} rev {}) handled in {} ms: scanned={} written={} skipped={} unresolvable={} "
				+ "enqueued={} duplicates={}", event.eventId(), event.type(), event.version(),
				event.lineageRevision(), result.elapsed().toMillis(), result.filesScanned(), result.pendingWritten(),
				result.pendingSkipped(), result.unresolvable(), result.tasksEnqueued(), result.tasksDuplicate());
		return result;
	}

	private ShardResult processShard(TemplateVersionEvent event, PendingUpdateCalculator calculator, int shard) {
		int scanned = 0;
		int applied = 0;
		int skipped = 0;
		int unresolvable = 0;
		int enqueued = 0;
		int duplicates = 0;
		String cursor = null;
		do {
			EngagementIndex.Page page = index.scan(event.templateId(), shard, config.shards(), cursor,
					config.scanBatchSize());
			Recomputed recomputed = recompute(page.files(), calculator);
			for (EngagementFile file : recomputed.needsLoad()) {
				switch (store.enqueue(loadTask(file, event))) {
					case ENQUEUED -> enqueued++;
					case DUPLICATE -> duplicates++;
				}
			}
			scanned += page.files().size();
			applied += recomputed.applied();
			skipped += recomputed.skipped();
			unresolvable += recomputed.unresolvable();
			cursor = page.nextCursor();
		}
		while (cursor != null);

		stats.add(Counter.FILES_SCANNED, scanned);
		stats.add(Counter.TASKS_ENQUEUED, enqueued);
		stats.add(Counter.TASKS_DUPLICATE, duplicates);
		return new ShardResult(scanned, applied, skipped, unresolvable, enqueued, duplicates);
	}

	/**
	 * Recomputes the pending rows of these files. The files that cannot be evaluated come back in {@code needsLoad}.
	 * An unresolvable baseline is demoted to UNKNOWN: a false "up to date" is the failure we cannot afford.
	 */
	private Recomputed recompute(List<EngagementFile> files, PendingUpdateCalculator calculator) {
		Instant now = clock.instant();
		long revision = calculator.graph().lineageRevision();
		List<PendingWrite> writes = new ArrayList<>();
		List<EngagementFile> needsLoad = new ArrayList<>();
		List<Long> unresolvable = new ArrayList<>();

		for (EngagementFile file : files) {
			if (file.state() == VerificationState.UNKNOWN) {
				needsLoad.add(file);
				continue;
			}
			Optional<Set<String>> pending = calculator.pendingFor(file.baselineVersion(), file.declinedVersions());
			if (pending.isPresent()) {
				writes.add(new PendingWrite(file.id(), file.templateId(), pending.get(), file.rowVersion(), revision,
						now));
			}
			else {
				unresolvable.add(file.id());
				needsLoad.add(file);
			}
		}

		int applied = writes.isEmpty() ? 0 : index.replacePending(writes);
		if (!unresolvable.isEmpty()) {
			index.markUnknown(unresolvable);
			log.warn("{} files have a baseline unknown to the version graph of template {}: marked UNKNOWN",
					unresolvable.size(), calculator.graph().templateId());
		}
		stats.add(Counter.PENDING_WRITTEN, applied);
		stats.add(Counter.PENDING_SKIPPED, writes.size() - applied);
		stats.add(Counter.UNRESOLVABLE_BASELINE, unresolvable.size());
		return new Recomputed(applied, writes.size() - applied, unresolvable.size(), needsLoad);
	}

	private static LoadTask loadTask(EngagementFile file, TemplateVersionEvent event) {
		TaskKey key = new TaskKey(file.id(), TaskKey.Operation.LOAD_VERIFY, event.version(), event.lineageRevision());
		return new LoadTask(key, file.firmId(), file.templateId());
	}

	// ---------------------------------------------------------------- LoadTaskHandler

	@Override
	public boolean isStillNeeded(LoadTask task) {
		return index.get(task.fileId()).map(file -> file.state() == VerificationState.UNKNOWN).orElse(false);
	}

	@Override
	public void onLoaded(LoadTask task, LoadResult result) {
		Optional<EngagementFile> verified = index.markVerified(result.fileId(), result.baselineVersion(),
				result.loadedAt());
		if (verified.isEmpty()) {
			return;
		}
		VersionGraph graph = graphs.load(task.templateId());
		recompute(List.of(verified.get()), new PendingUpdateCalculator(graph));
	}

	private static <T> T await(Future<T> future) {
		try {
			return future.get();
		}
		catch (ExecutionException e) {
			if (e.getCause() instanceof RuntimeException runtime) {
				throw runtime;
			}
			throw new IllegalStateException("fan-out shard failed", e.getCause());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted while fanning out", e);
		}
	}
}
