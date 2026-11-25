package com.invencible.labs.cw.fanout.engine;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.function.Consumer;

import com.invencible.labs.cw.fanout.model.TemplateVersionEvent;
import com.invencible.labs.cw.fanout.model.VerificationState;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex;
import com.invencible.labs.cw.fanout.sim.InMemoryVersionGraphSource;
import com.invencible.labs.cw.fanout.sim.SimulatedFileLoader;

/** The whole worker wired with real threads and short durations, against the simulated downstream. */
final class SchedulerRig implements AutoCloseable {

	static final long TEMPLATE = 1;

	static final class Settings {
		Duration latency = Duration.ofMillis(20);
		int downstreamCapacity = 100;
		Duration retryAfter = Duration.ofMillis(10);
		int maxConcurrency = 8;
		int initialConcurrency = 8;
		int perFirmCap = 100;
		int maxAttempts = 5;
		int breakerThreshold = 3;
		Duration breakerOpenFor = Duration.ofMillis(100);
		Duration breakerMaxOpenFor = Duration.ofMillis(200);
		Duration lease = Duration.ofSeconds(30);
		Duration shutdownGrace = Duration.ofSeconds(2);
	}

	final Clock clock = Clock.systemUTC();
	final InMemoryEngagementIndex index = new InMemoryEngagementIndex();
	final InMemoryVersionGraphSource graphs = new InMemoryVersionGraphSource();
	final SimulatedFileLoader loader;
	final InMemoryTaskStore store;
	final AimdLimiter limiter;
	final CircuitBreaker breaker;
	final FanOutStats stats = new FanOutStats();
	final FanOutWorker worker;
	final LoadScheduler scheduler;
	private final long revision;
	private long nextFile = 1;

	SchedulerRig() {
		this(settings -> { });
	}

	SchedulerRig(Consumer<Settings> customize) {
		Settings s = new Settings();
		customize.accept(s);
		loader = new SimulatedFileLoader(
				new SimulatedFileLoader.Behaviour(s.latency, s.downstreamCapacity, s.retryAfter, 0, 0), clock);
		store = new InMemoryTaskStore(s.perFirmCap, s.maxAttempts, s.lease, clock);
		limiter = new AimdLimiter(1, s.initialConcurrency, s.maxConcurrency, 0.5, 0.9, Duration.ofMillis(50), clock);
		breaker = new CircuitBreaker(s.breakerThreshold, s.breakerOpenFor, s.breakerMaxOpenFor, clock);
		worker = new FanOutWorker(graphs, index, store, new FanOutWorker.Config(4, 50), clock, stats);
		scheduler = new LoadScheduler(store, loader, worker, limiter, new TokenBucket(10_000, 1_000, clock), breaker,
				new Backoff(Duration.ofMillis(5), Duration.ofMillis(20), new SplittableRandom(1)),
				new LoadScheduler.Options(Duration.ofMillis(5), Duration.ofMillis(300), s.shutdownGrace), clock, stats);
		graphs.publish(TEMPLATE, "v1", "default");
		revision = graphs.publish(TEMPLATE, "v2", "default", "v1");
	}

	/** Files whose baseline is unverified: the ones that need a load. Pending: v2 once verified. */
	List<Long> unverifiedFiles(long firm, int count) {
		List<Long> ids = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			long id = nextFile++;
			index.add(id, firm, TEMPLATE, "v1", VerificationState.UNKNOWN, Set.of());
			loader.setActualBaseline(id, "v1");
			ids.add(id);
		}
		return ids;
	}

	TemplateVersionEvent event() {
		return new TemplateVersionEvent("evt-" + revision, TemplateVersionEvent.Type.PUBLISHED, 1, TEMPLATE, "v2",
				List.of("v1"), "default", revision, clock.instant());
	}

	boolean drain() throws InterruptedException {
		scheduler.start();
		return scheduler.awaitIdle(Duration.ofSeconds(30));
	}

	@Override
	public void close() {
		scheduler.close();
	}
}
