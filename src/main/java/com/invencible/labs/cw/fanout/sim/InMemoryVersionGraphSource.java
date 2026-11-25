package com.invencible.labs.cw.fanout.sim;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.invencible.labs.cw.fanout.model.VersionGraph;
import com.invencible.labs.cw.fanout.model.VersionGraph.Node;
import com.invencible.labs.cw.fanout.model.VersionGraph.Status;
import com.invencible.labs.cw.fanout.port.VersionGraphSource;

/** A stand-in for the Product Engine API: a mutable catalog that the demo uses to publish and withdraw versions. */
public final class InMemoryVersionGraphSource implements VersionGraphSource {

	private final Map<Long, Map<String, Node>> templates = new HashMap<>();
	private final Map<Long, Long> revisions = new HashMap<>();

	/** @return the new lineage revision, to put in the event */
	public synchronized long publish(long templateId, String version, String market, String... parents) {
		templates.computeIfAbsent(templateId, id -> new HashMap<>())
				.put(version, new Node(version, market, Status.PUBLISHED, Set.of(parents)));
		return bump(templateId);
	}

	/** @return the new lineage revision, to put in the event */
	public synchronized long withdraw(long templateId, String version) {
		Map<String, Node> versions = templates.get(templateId);
		Node node = versions == null ? null : versions.get(version);
		if (node == null) {
			throw new IllegalArgumentException("unknown version " + version);
		}
		versions.put(version, new Node(version, node.market(), Status.WITHDRAWN, node.parents()));
		return bump(templateId);
	}

	@Override
	public synchronized VersionGraph load(long templateId) {
		Map<String, Node> versions = templates.getOrDefault(templateId, Map.of());
		return new VersionGraph(templateId, revisions.getOrDefault(templateId, 0L), versions.values());
	}

	private long bump(long templateId) {
		return revisions.merge(templateId, 1L, Long::sum);
	}
}
