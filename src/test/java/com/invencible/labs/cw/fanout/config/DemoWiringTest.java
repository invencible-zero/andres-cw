package com.invencible.labs.cw.fanout.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.invencible.labs.cw.fanout.engine.FanOutStats;
import com.invencible.labs.cw.fanout.engine.FanOutStats.Counter;
import com.invencible.labs.cw.fanout.port.TaskStore;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex.Status;
import com.invencible.labs.cw.fanout.sim.SimulatedFileLoader;

/** Boots the real Spring wiring with the demo profile and a tiny world, and checks that the demo ran to the end. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"fanout.demo.giant-firm-files=40",
		"fanout.demo.medium-firms=1",
		"fanout.demo.medium-firm-files=10",
		"fanout.demo.small-firms=2",
		"fanout.demo.small-firm-files=4",
		"fanout.demo.poison-files=2",
		"fanout.demo.load-latency=15ms",
		"fanout.demo.outage-after=60ms",
		"fanout.demo.outage-for=150ms",
		"fanout.demo.exit-when-done=false" })
@ActiveProfiles("demo")
class DemoWiringTest {

	@Autowired
	private TaskStore store;

	@Autowired
	private InMemoryEngagementIndex index;

	@Autowired
	private SimulatedFileLoader loader;

	@Autowired
	private FanOutStats stats;

	@Test
	void theDemoDrainedTheQueueAndOnlyTheUnloadableFilesRemainUnknown() {
		assertThat(store.counts().idle()).isTrue();

		long unloadable = store.deadLetters().stream().map(d -> d.task().fileId()).distinct().count();
		assertThat(unloadable).isEqualTo(2);
		assertThat(index.statusCounts().getOrDefault(Status.UNKNOWN, 0L))
				.as("a file that could not be verified is never shown as up to date").isEqualTo(unloadable);
	}

	@Test
	void noFileWasLoadedTwiceAndTheNumbersAddUp() {
		assertThat(loader.filesLoadedMoreThanOnce()).isEmpty();
		assertThat(stats.get(Counter.EVENTS_HANDLED)).as("publish, replay, withdrawal").isEqualTo(3);
		assertThat(stats.get(Counter.TASKS_DUPLICATE)).as("the replayed event").isPositive();
	}
}
