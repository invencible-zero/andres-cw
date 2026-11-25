package com.invencible.labs.cw.fanout.config;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.invencible.labs.cw.fanout.engine.AimdLimiter;
import com.invencible.labs.cw.fanout.engine.CircuitBreaker;
import com.invencible.labs.cw.fanout.engine.FanOutStats;
import com.invencible.labs.cw.fanout.engine.FanOutStats.Counter;
import com.invencible.labs.cw.fanout.engine.FanOutWorker;
import com.invencible.labs.cw.fanout.engine.FanOutWorker.HandleResult;
import com.invencible.labs.cw.fanout.engine.LoadScheduler;
import com.invencible.labs.cw.fanout.model.TemplateVersionEvent;
import com.invencible.labs.cw.fanout.model.VerificationState;
import com.invencible.labs.cw.fanout.port.TaskStore;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex;
import com.invencible.labs.cw.fanout.sim.InMemoryVersionGraphSource;
import com.invencible.labs.cw.fanout.sim.SimulatedFileLoader;

/**
 * Plays one publish end to end against a simulated world and prints what happened:
 * <ol>
 *   <li>v3 is published. The fast path recomputes every indexed file and enqueues loads for the unverified ones.</li>
 *   <li>The same event is delivered again, as at-least-once delivery will do.</li>
 *   <li>The queue drains against a downstream that is slower than us, throttles, times out, goes down and
 *       has files it can never load.</li>
 *   <li>v3 is withdrawn, and the pending rows follow.</li>
 * </ol>
 * Time is compressed: a load that takes a minute in production takes a few hundred milliseconds here.
 */
public final class DemoRunner implements ApplicationRunner {

	private static final long TEMPLATE = 1L;
	private static final long PRODUCT = 1L;

	private final DemoProperties demo;
	private final FanOutProperties config;
	private final InMemoryEngagementIndex index;
	private final InMemoryVersionGraphSource graphs;
	private final SimulatedFileLoader loader;
	private final FanOutWorker worker;
	private final LoadScheduler scheduler;
	private final TaskStore store;
	private final AimdLimiter limiter;
	private final CircuitBreaker breaker;
	private final FanOutStats stats;
	private final ConfigurableApplicationContext context;

	private final Map<Long, String> firmKinds = new LinkedHashMap<>();
	private final Map<Long, Integer> poisonByFirm = new HashMap<>();
	private long unverified;
	private long sampleFile = -1;
	private long sampleDeclinedFile = -1;

	public DemoRunner(DemoProperties demo, FanOutProperties config, InMemoryEngagementIndex index,
			InMemoryVersionGraphSource graphs, SimulatedFileLoader loader, FanOutWorker worker,
			LoadScheduler scheduler, TaskStore store, AimdLimiter limiter, CircuitBreaker breaker, FanOutStats stats,
			ConfigurableApplicationContext context) {
		this.demo = demo;
		this.config = config;
		this.index = index;
		this.graphs = graphs;
		this.loader = loader;
		this.worker = worker;
		this.scheduler = scheduler;
		this.store = store;
		this.limiter = limiter;
		this.breaker = breaker;
		this.stats = stats;
		this.context = context;
	}

	@Override
	public void run(ApplicationArguments args) {
		int exitCode = 0;
		try {
			play();
		}
		catch (Exception e) {
			e.printStackTrace();
			exitCode = 1;
		}
		if (demo.exitWhenDone()) {
			int code = exitCode;
			System.exit(SpringApplication.exit(context, () -> code));
		}
	}

	private void play() throws InterruptedException {
		seedWorld();
		say("");
		say("World: %d firms, %d engagement files, %d of them unverified. Downstream: a load takes %d ms (stands for ~60 s), "
				.formatted(firmKinds.size(), index.size(), unverified, demo.loadLatency().toMillis())
				+ "it can serve %d at once, we may use up to %d.".formatted(demo.downstreamCapacity(),
						config.maxConcurrency()));

		// 1. publish
		long revision = graphs.publish(TEMPLATE, "v3", "default", "v2");
		TemplateVersionEvent publish = event("evt-publish-v3", TemplateVersionEvent.Type.PUBLISHED, revision);
		say("");
		say("== 1. v3 is published: fast path, no downstream call ==");
		HandleResult first = worker.handle(publish);
		say("   scanned %d files in %d shards in %d ms -> %d pending rows written, %d load tasks enqueued"
				.formatted(first.filesScanned(), config.shards(), first.elapsed().toMillis(), first.pendingWritten(),
						first.tasksEnqueued()));
		say("   indicator right now: " + index.statusCounts());

		// 2. duplicate delivery
		say("");
		say("== 2. the same event is delivered again ==");
		HandleResult again = worker.handle(publish);
		say("   %d new tasks (%d duplicates ignored), %d pending writes skipped (already computed at this revision)"
				.formatted(again.tasksEnqueued(), again.tasksDuplicate(), again.pendingSkipped()));

		// 3. drain
		say("");
		say("== 3. draining %d load tasks into the slow downstream =="
				.formatted(store.counts().queued()));
		Thread outage = Thread.ofVirtual().start(this::simulateOutage);
		Map<Long, Double> firmDoneAt = drain();
		outage.interrupt();
		report(firmDoneAt);

		// 4. withdrawal
		say("");
		say("== 4. v3 is withdrawn: pending rows are recomputed from the graph, not patched ==");
		Set<String> before = index.pendingOf(sampleFile);
		Set<String> beforeDeclined = index.pendingOf(sampleDeclinedFile);
		long revision2 = graphs.withdraw(TEMPLATE, "v3");
		HandleResult withdrawn = worker.handle(event("evt-withdraw-v3", TemplateVersionEvent.Type.WITHDRAWN,
				revision2));
		say("   file %d (baseline v1):                 pending %s -> %s".formatted(sampleFile, before,
				index.pendingOf(sampleFile)));
		say("   file %d (baseline v2, declined v2-eu): pending %s -> %s  (the decline pinned v2-eu only)"
				.formatted(sampleDeclinedFile, beforeDeclined, index.pendingOf(sampleDeclinedFile)));
		say("   %d pending rows rewritten; the %d files still UNKNOWN got one new load task each"
				.formatted(withdrawn.pendingWritten(), withdrawn.tasksEnqueued()));
		scheduler.awaitIdle(Duration.ofSeconds(30));
		say("");
	}

	// ---------------------------------------------------------------- the drain

	private Map<Long, Double> drain() throws InterruptedException {
		Map<Long, Double> firmDoneAt = new HashMap<>();
		long start = System.nanoTime();
		long lastLine = 0;
		while (!store.counts().idle() && seconds(start) < 300) {
			Thread.sleep(100);
			double now = seconds(start);
			Map<Long, Long> unknown = index.unknownCountByFirm();
			for (Long firm : firmKinds.keySet()) {
				if (!firmDoneAt.containsKey(firm) && unknown.getOrDefault(firm, 0L) <= poisonByFirm.getOrDefault(firm, 0)) {
					firmDoneAt.put(firm, now);
				}
			}
			if (now - lastLine >= 1.0) {
				lastLine = (long) now;
				TaskStore.Counts counts = store.counts();
				say("   [%5.1fs] limit=%-3d in-flight=%-3d queued=%-5d done=%-5d | 429=%-3d timeout=%-3d 5xx=%-3d | breaker=%s"
						.formatted(now, limiter.limit(), limiter.inFlight(), counts.queued(), counts.done(),
								stats.get(Counter.LOADS_THROTTLED), stats.get(Counter.LOADS_TIMED_OUT),
								stats.get(Counter.LOADS_UNAVAILABLE), breaker.state()));
			}
		}
		firmDoneAt.putIfAbsent(-1L, seconds(start));
		return firmDoneAt;
	}

	private void simulateOutage() {
		try {
			Thread.sleep(demo.outageAfter());
			loader.setOutage(true);
			say("   >>> downstream goes DOWN for %d ms".formatted(demo.outageFor().toMillis()));
			Thread.sleep(demo.outageFor());
			loader.setOutage(false);
			say("   >>> downstream is back");
		}
		catch (InterruptedException e) {
			loader.setOutage(false);
			Thread.currentThread().interrupt();
		}
	}

	// ---------------------------------------------------------------- the report

	private void report(Map<Long, Double> firmDoneAt) {
		double total = firmDoneAt.remove(-1L);
		say("");
		say("   -- result after %.1f s --".formatted(total));
		say("   loads:        ok=%d  skipped-not-needed=%d  throttled(429)=%d  timeouts=%d  5xx=%d  rejected=%d"
				.formatted(stats.get(Counter.LOADS_OK), stats.get(Counter.LOADS_SKIPPED_NOT_NEEDED),
						stats.get(Counter.LOADS_THROTTLED), stats.get(Counter.LOADS_TIMED_OUT),
						stats.get(Counter.LOADS_UNAVAILABLE), stats.get(Counter.LOADS_REJECTED)));
		say("   retries=%d  dead-lettered=%d  expired leases=%d".formatted(stats.get(Counter.RETRIES),
				stats.get(Counter.DEAD_LETTERED), stats.get(Counter.LEASES_EXPIRED)));
		say("   capacity:     downstream peak concurrency %d (its limit %d, our maximum %d); our limit ended at %d "
				.formatted(loader.peakConcurrency(), demo.downstreamCapacity(), config.maxConcurrency(),
						limiter.limit())
				+ "after %d decreases; it rejected %d calls for capacity; breaker tripped %d time(s); %d calls made during the outage"
						.formatted(limiter.decreases(), loader.rejectedForCapacity(), breaker.trips(),
								loader.callsDuringOutage()));

		Map<String, List<Double>> byKind = new LinkedHashMap<>();
		firmDoneAt.forEach((firm, at) -> byKind.computeIfAbsent(firmKinds.get(firm), k -> new ArrayList<>()).add(at));
		byKind.values().forEach(Collections::sort);
		StringBuilder fairness = new StringBuilder("   fairness:     time until every firm was done: ");
		byKind.forEach((kind, times) -> fairness.append("%s median %.1fs, last %.1fs;  "
				.formatted(kind, times.get(times.size() / 2), times.get(times.size() - 1))));
		say(fairness.toString().stripTrailing());

		List<Long> twice = loader.filesLoadedMoreThanOnce();
		say("   idempotency:  %d files loaded successfully more than once%s; %d extra calls were retries"
				.formatted(twice.size(), twice.isEmpty() ? " (good)" : " " + twice,
						loader.totalCalls() - loader.callCounts().size()));
		say("   dead letters: %d (the poison files, one task each, reason: %s)".formatted(store.deadLetters().size(),
				store.deadLetters().isEmpty() ? "-" : store.deadLetters().get(0).reason()));
		say("   indicator:    " + index.statusCounts()
				+ "  <- UNKNOWN is exactly the files that could not be verified; none is shown as up to date");
	}

	// ---------------------------------------------------------------- the simulated world

	private void seedWorld() {
		graphs.publish(TEMPLATE, "v1", "default");
		graphs.publish(TEMPLATE, "v2", "default", "v1");
		graphs.publish(TEMPLATE, "v2-eu", "eu", "v2");

		Random random = new Random(42);
		List<Long> unverified = new ArrayList<>();
		long[] ids = { 0, 0 };
		seedFirm(ids, "giant", demo.giantFirmFiles(), random, unverified);
		for (int i = 0; i < demo.mediumFirms(); i++) {
			seedFirm(ids, "medium", demo.mediumFirmFiles(), random, unverified);
		}
		for (int i = 0; i < demo.smallFirms(); i++) {
			seedFirm(ids, "small", demo.smallFirmFiles(), random, unverified);
		}

		this.unverified = unverified.size();
		Collections.shuffle(unverified, random);
		Set<Long> poison = new HashSet<>(unverified.subList(0, Math.min(demo.poisonFiles(), unverified.size())));
		for (long file : poison) {
			loader.poison(file);
			poisonByFirm.merge(index.get(file).orElseThrow().firmId(), 1, Integer::sum);
		}
	}

	/** ids[0] is the last file id, ids[1] the last firm id. */
	private void seedFirm(long[] ids, String kind, int files, Random random, List<Long> unverified) {
		long firm = ++ids[1];
		firmKinds.put(firm, kind);
		for (int i = 0; i < files; i++) {
			long id = ++ids[0];
			// v3 branches from v2, so a file already on the v2-eu branch is not behind it
			double roll = random.nextDouble();
			String baseline = roll < 0.5 ? "v1" : roll < 0.85 ? "v2" : "v2-eu";
			boolean unknown = random.nextDouble() < demo.unknownRatio();
			Set<String> declined = !unknown && baseline.equals("v2") && random.nextDouble() < 0.2
					? Set.of("v2-eu") : Set.of();
			index.add(id, firm, TEMPLATE, baseline, unknown ? VerificationState.UNKNOWN : VerificationState.VERIFIED,
					declined);
			if (unknown) {
				unverified.add(id);
				// 10% of the unverified v1 files have in fact moved to v2: only a load can reveal that
				loader.setActualBaseline(id, baseline.equals("v1") && random.nextDouble() < 0.1 ? "v2" : baseline);
			}
			if (sampleFile < 0 && !unknown && baseline.equals("v1")) {
				sampleFile = id;
			}
			if (sampleDeclinedFile < 0 && !declined.isEmpty()) {
				sampleDeclinedFile = id;
			}
		}
	}

	private TemplateVersionEvent event(String id, TemplateVersionEvent.Type type, long revision) {
		return new TemplateVersionEvent(id, type, PRODUCT, TEMPLATE, "v3", List.of("v2"), "default", revision,
				Instant.now());
	}

	private static double seconds(long startNanos) {
		return (System.nanoTime() - startNanos) / 1e9;
	}

	private static void say(String line) {
		System.out.println(line);
	}
}
