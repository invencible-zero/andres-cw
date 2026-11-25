package com.invencible.labs.cw.fanout.config;

import java.time.Clock;
import java.util.random.RandomGenerator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.invencible.labs.cw.fanout.engine.AimdLimiter;
import com.invencible.labs.cw.fanout.engine.Backoff;
import com.invencible.labs.cw.fanout.engine.CircuitBreaker;
import com.invencible.labs.cw.fanout.engine.FanOutStats;
import com.invencible.labs.cw.fanout.engine.FanOutWorker;
import com.invencible.labs.cw.fanout.engine.InMemoryTaskStore;
import com.invencible.labs.cw.fanout.engine.LoadScheduler;
import com.invencible.labs.cw.fanout.engine.TokenBucket;
import com.invencible.labs.cw.fanout.port.EngagementIndex;
import com.invencible.labs.cw.fanout.port.FileLoader;
import com.invencible.labs.cw.fanout.port.TaskStore;
import com.invencible.labs.cw.fanout.port.VersionGraphSource;

/**
 * Wires the engine to its ports. Off unless {@code fanout.enabled=true}, because iteration 1 has no production
 * adapters yet: the {@link EngagementIndex}, {@link VersionGraphSource} and {@link FileLoader} beans come from the
 * {@code demo} profile.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "fanout", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(FanOutProperties.class)
public class FanOutConfiguration {

	@Bean
	Clock fanOutClock() {
		return Clock.systemUTC();
	}

	@Bean
	FanOutStats fanOutStats() {
		return new FanOutStats();
	}

	@Bean
	TaskStore fanOutTaskStore(FanOutProperties p, Clock clock) {
		return new InMemoryTaskStore(p.maxInFlightPerFirm(), p.maxAttempts(), p.lease(), clock);
	}

	@Bean
	AimdLimiter fanOutLimiter(FanOutProperties p, Clock clock) {
		return new AimdLimiter(p.minConcurrency(), p.initialConcurrency(), p.maxConcurrency(), p.throttleRatio(),
				p.timeoutRatio(), p.decreaseCooldown(), clock);
	}

	@Bean
	TokenBucket fanOutBucket(FanOutProperties p, Clock clock) {
		return new TokenBucket(p.startRatePerSecond(), p.startBurst(), clock);
	}

	@Bean
	CircuitBreaker fanOutBreaker(FanOutProperties p, Clock clock) {
		return new CircuitBreaker(p.breakerFailureThreshold(), p.breakerOpenFor(), p.breakerMaxOpenFor(), clock);
	}

	@Bean
	Backoff fanOutBackoff(FanOutProperties p) {
		return new Backoff(p.retryBase(), p.retryCap(), RandomGenerator.getDefault());
	}

	@Bean
	FanOutWorker fanOutWorker(VersionGraphSource graphs, EngagementIndex index, TaskStore store,
			FanOutProperties p, Clock clock, FanOutStats stats) {
		return new FanOutWorker(graphs, index, store, new FanOutWorker.Config(p.shards(), p.scanBatchSize()), clock,
				stats);
	}

	@Bean(initMethod = "start", destroyMethod = "close")
	LoadScheduler fanOutScheduler(TaskStore store, FileLoader loader, FanOutWorker worker, AimdLimiter limiter,
			TokenBucket bucket, CircuitBreaker breaker, Backoff backoff, FanOutProperties p, Clock clock,
			FanOutStats stats) {
		return new LoadScheduler(store, loader, worker, limiter, bucket, breaker, backoff,
				new LoadScheduler.Options(p.tick(), p.callTimeout(), p.shutdownGrace()), clock, stats);
	}
}
