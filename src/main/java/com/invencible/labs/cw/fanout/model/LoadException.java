package com.invencible.labs.cw.fanout.model;

import java.time.Duration;

/**
 * The only way a {@code FileLoader} reports a downstream failure. The {@link Kind} is the whole contract: it decides
 * how the worker reacts (retry or not, slow down or not, count against the task or not).
 */
public final class LoadException extends RuntimeException {

	public enum Kind {
		/** 429 or 503 with Retry-After. The downstream is protecting itself: slow down, do not blame the task. */
		THROTTLED,
		/** The deadline expired. Ambiguous (the load may have happened). It is an overload signal and a task failure. */
		TIMEOUT,
		/** 5xx or connection failure. Transient: retry the task, and count towards the circuit breaker. */
		UNAVAILABLE,
		/** 4xx: this file cannot be loaded (missing, corrupt, forbidden). Retrying cannot help: dead-letter it. */
		REJECTED
	}

	private final Kind kind;
	private final Duration retryAfter;

	private LoadException(Kind kind, String message, Duration retryAfter, Throwable cause) {
		super(message, cause);
		this.kind = kind;
		this.retryAfter = retryAfter;
	}

	public static LoadException throttled(Duration retryAfter) {
		return new LoadException(Kind.THROTTLED, "throttled, retry after " + retryAfter, retryAfter, null);
	}

	public static LoadException timeout(String message) {
		return new LoadException(Kind.TIMEOUT, message, Duration.ZERO, null);
	}

	public static LoadException unavailable(String message, Throwable cause) {
		return new LoadException(Kind.UNAVAILABLE, message, Duration.ZERO, cause);
	}

	public static LoadException rejected(String message) {
		return new LoadException(Kind.REJECTED, message, Duration.ZERO, null);
	}

	public Kind kind() {
		return kind;
	}

	/** The downstream's own Retry-After hint, or zero when it gave none. */
	public Duration retryAfter() {
		return retryAfter;
	}
}
