package com.invencible.labs.cw.fanout.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pending updates as a pure function of {@code (baseline, declined, version graph)} (ADD section 3). Nothing is stored
 * as a delta, so duplicated, late or reordered events converge to the same answer.
 *
 * <p>A version is pending for a file when it is published, descends from the baseline, was not declined for that
 * file, and is not an ancestor of another candidate. Updates that accumulated therefore collapse into the latest
 * candidate per branch. A decline pins only that one version: a later version that contains it is still offered.
 *
 * <p>One instance serves one graph snapshot. Thousands of files share a handful of distinct inputs, so results are
 * memoised.
 */
public final class PendingUpdateCalculator {

	private record Inputs(String baseline, Set<String> declined) {
	}

	private final VersionGraph graph;
	private final Map<Inputs, Optional<Set<String>>> memo = new ConcurrentHashMap<>();

	public PendingUpdateCalculator(VersionGraph graph) {
		this.graph = graph;
	}

	public VersionGraph graph() {
		return graph;
	}

	/**
	 * @return the pending target versions, or empty when the baseline is not in the graph. Empty is "cannot
	 *         evaluate", never "up to date": a false "up to date" is the silent failure this design fears most.
	 */
	public Optional<Set<String>> pendingFor(String baseline, Set<String> declined) {
		return memo.computeIfAbsent(new Inputs(baseline, Set.copyOf(declined)), this::compute);
	}

	private Optional<Set<String>> compute(Inputs in) {
		if (!graph.contains(in.baseline())) {
			return Optional.empty();
		}
		List<String> candidates = graph.published().stream()
				.map(VersionGraph.Node::version)
				.filter(v -> !v.equals(in.baseline()))
				.filter(v -> !in.declined().contains(v))
				.filter(v -> graph.ancestors(v).contains(in.baseline()))
				.toList();

		Set<String> latestPerBranch = new TreeSet<>();
		for (String candidate : candidates) {
			boolean hidden = candidates.stream()
					.anyMatch(other -> !other.equals(candidate) && graph.ancestors(other).contains(candidate));
			if (!hidden) {
				latestPerBranch.add(candidate);
			}
		}
		return Optional.of(Collections.unmodifiableSet(latestPerBranch));
	}
}
