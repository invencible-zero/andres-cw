package com.invencible.labs.cw.fanout.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Every knob of the worker. The defaults are sized for the real downstream (~1 minute per load). The concurrency
 * numbers are the ones to agree with the team that owns the load operation. {@code maxConcurrency} is the share of
 * their capacity we may use, and it must leave headroom for the interactive loads of users opening files.
 */
@ConfigurationProperties(prefix = "fanout")
public record FanOutProperties(
		@DefaultValue("false") boolean enabled,

		// downstream capacity
		@DefaultValue("100") int maxConcurrency,
		@DefaultValue("20") int initialConcurrency,
		@DefaultValue("2") int minConcurrency,
		@DefaultValue("0.5") double throttleRatio,
		@DefaultValue("0.9") double timeoutRatio,
		@DefaultValue("10s") Duration decreaseCooldown,
		@DefaultValue("2.0") double startRatePerSecond,
		@DefaultValue("5") int startBurst,
		@DefaultValue("25") int maxInFlightPerFirm,

		// one call
		@DefaultValue("90s") Duration callTimeout,
		@DefaultValue("3m") Duration lease,
		@DefaultValue("30s") Duration shutdownGrace,

		// failure handling
		@DefaultValue("5") int maxAttempts,
		@DefaultValue("30s") Duration retryBase,
		@DefaultValue("10m") Duration retryCap,
		@DefaultValue("5") int breakerFailureThreshold,
		@DefaultValue("30s") Duration breakerOpenFor,
		@DefaultValue("2m") Duration breakerMaxOpenFor,

		// fast path
		@DefaultValue("8") int shards,
		@DefaultValue("1000") int scanBatchSize,
		@DefaultValue("250ms") Duration tick) {
}
