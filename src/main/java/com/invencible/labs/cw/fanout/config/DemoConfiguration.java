package com.invencible.labs.cw.fanout.config;

import java.time.Clock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.invencible.labs.cw.fanout.engine.AimdLimiter;
import com.invencible.labs.cw.fanout.engine.CircuitBreaker;
import com.invencible.labs.cw.fanout.engine.FanOutStats;
import com.invencible.labs.cw.fanout.engine.FanOutWorker;
import com.invencible.labs.cw.fanout.engine.LoadScheduler;
import com.invencible.labs.cw.fanout.port.TaskStore;
import com.invencible.labs.cw.fanout.sim.InMemoryEngagementIndex;
import com.invencible.labs.cw.fanout.sim.InMemoryVersionGraphSource;
import com.invencible.labs.cw.fanout.sim.SimulatedFileLoader;

/** The simulated adapters and the demo itself. Only with {@code --spring.profiles.active=demo}. */
@Configuration(proxyBeanMethods = false)
@Profile("demo")
@EnableConfigurationProperties(DemoProperties.class)
public class DemoConfiguration {

	@Bean
	InMemoryEngagementIndex demoIndex() {
		return new InMemoryEngagementIndex();
	}

	@Bean
	InMemoryVersionGraphSource demoGraphs() {
		return new InMemoryVersionGraphSource();
	}

	@Bean
	SimulatedFileLoader demoLoader(DemoProperties p, Clock clock) {
		return new SimulatedFileLoader(new SimulatedFileLoader.Behaviour(p.loadLatency(), p.downstreamCapacity(),
				p.retryAfter(), p.timeoutRate(), p.unavailableRate()), clock);
	}

	@Bean
	DemoRunner demoRunner(DemoProperties demo, FanOutProperties config, InMemoryEngagementIndex index,
			InMemoryVersionGraphSource graphs, SimulatedFileLoader loader, FanOutWorker worker,
			LoadScheduler scheduler, TaskStore store, AimdLimiter limiter, CircuitBreaker breaker,
			FanOutStats stats, ConfigurableApplicationContext context) {
		return new DemoRunner(demo, config, index, graphs, loader, worker, scheduler, store, limiter, breaker, stats,
				context);
	}
}
