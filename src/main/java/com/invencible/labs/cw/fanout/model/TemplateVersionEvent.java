package com.invencible.labs.cw.fanout.model;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * The event contract of the Integration Domain (ADD section 2.3). It carries identifiers, never content.
 *
 * <p>Delivery is at-least-once: the same event can arrive twice, late, or after a newer one. Nothing in the worker
 * relies on event order. It always recomputes from the current version graph.
 */
public record TemplateVersionEvent(
		String eventId,
		Type type,
		long productId,
		long templateId,
		String version,
		List<String> parents,
		String market,
		long lineageRevision,
		Instant occurredAt) {

	public enum Type { PUBLISHED, WITHDRAWN }

	public TemplateVersionEvent {
		Objects.requireNonNull(eventId, "eventId");
		Objects.requireNonNull(type, "type");
		Objects.requireNonNull(version, "version");
		Objects.requireNonNull(market, "market");
		Objects.requireNonNull(occurredAt, "occurredAt");
		parents = List.copyOf(parents);
	}
}
