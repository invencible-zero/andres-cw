package com.invencible.labs.cw.fanout.model;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An immutable snapshot of the template version graph (ADD section 3). Versions form a graph, not a line: a version
 * has zero or more parents, belongs to a market and can be withdrawn after publication.
 *
 * <p>The snapshot carries the {@code lineageRevision} it was read at. Derived rows are stamped with it so that a
 * slow worker holding an older graph can never overwrite the result of a newer one.
 */
public final class VersionGraph {

	public enum Status { PUBLISHED, WITHDRAWN }

	public record Node(String version, String market, Status status, Set<String> parents) {
		public Node {
			parents = Set.copyOf(parents);
		}
	}

	private final long templateId;
	private final long lineageRevision;
	private final Map<String, Node> nodes = new HashMap<>();
	private final List<Node> published;
	private final Map<String, Set<String>> ancestorCache = new ConcurrentHashMap<>();

	public VersionGraph(long templateId, long lineageRevision, Collection<Node> nodes) {
		this.templateId = templateId;
		this.lineageRevision = lineageRevision;
		nodes.forEach(n -> this.nodes.put(n.version(), n));
		this.published = nodes.stream().filter(n -> n.status() == Status.PUBLISHED).toList();
	}

	public long templateId() {
		return templateId;
	}

	public long lineageRevision() {
		return lineageRevision;
	}

	public boolean contains(String version) {
		return nodes.containsKey(version);
	}

	/** Published versions only: what a firm can be offered. */
	public List<Node> published() {
		return published;
	}

	/**
	 * Strict ancestors of a version. A withdrawn version still carries lineage, so the walk goes through it: v7 can
	 * contain v5 even if v6, in between, was withdrawn. The graph is a DAG by contract; the visited set only makes the
	 * walk terminate on corrupt data.
	 */
	public Set<String> ancestors(String version) {
		return ancestorCache.computeIfAbsent(version, this::walkUp);
	}

	private Set<String> walkUp(String version) {
		Set<String> seen = new HashSet<>();
		ArrayDeque<String> todo = new ArrayDeque<>();
		Node start = nodes.get(version);
		if (start != null) {
			todo.addAll(start.parents());
		}
		while (!todo.isEmpty()) {
			String next = todo.poll();
			if (seen.add(next)) {
				Node node = nodes.get(next);
				if (node != null) {
					todo.addAll(node.parents());
				}
			}
		}
		return Set.copyOf(seen);
	}
}
