package com.invencible.labs.cw.fanout.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The simulated world of the demo: firms with a skewed number of files and a downstream that misbehaves. The real
 * load takes ~60 s. The demo compresses time (a load takes {@code loadLatency}), so minutes of behaviour fit in seconds.
 */
@ConfigurationProperties(prefix = "fanout.demo")
public record DemoProperties(
		@DefaultValue("1500") int giantFirmFiles,
		@DefaultValue("5") int mediumFirms,
		@DefaultValue("60") int mediumFirmFiles,
		@DefaultValue("60") int smallFirms,
		@DefaultValue("6") int smallFirmFiles,
		@DefaultValue("0.4") double unknownRatio,
		@DefaultValue("8") int poisonFiles,

		@DefaultValue("400ms") Duration loadLatency,
		@DefaultValue("24") int downstreamCapacity,
		@DefaultValue("500ms") Duration retryAfter,
		@DefaultValue("0.02") double timeoutRate,
		@DefaultValue("0.01") double unavailableRate,
		@DefaultValue("4s") Duration outageAfter,
		@DefaultValue("2500ms") Duration outageFor,

		@DefaultValue("true") boolean exitWhenDone) {
}
