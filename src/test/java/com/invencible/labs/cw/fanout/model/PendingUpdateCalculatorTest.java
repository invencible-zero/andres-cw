package com.invencible.labs.cw.fanout.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.invencible.labs.cw.fanout.model.VersionGraph.Node;
import com.invencible.labs.cw.fanout.model.VersionGraph.Status;

/** The scenarios of the case statement: branching markets, withdrawals, and "declined v5, later offered v7". */
class PendingUpdateCalculatorTest {

	private static Node published(String version, String... parents) {
		return new Node(version, "default", Status.PUBLISHED, Set.of(parents));
	}

	private static Node withdrawn(String version, String... parents) {
		return new Node(version, "default", Status.WITHDRAWN, Set.of(parents));
	}

	private static Optional<Set<String>> pending(String baseline, Set<String> declined, Node... nodes) {
		return new PendingUpdateCalculator(new VersionGraph(1, 1, java.util.List.of(nodes)))
				.pendingFor(baseline, declined);
	}

	@Test
	void accumulatedUpdatesCollapseIntoTheLatestOne() {
		Node[] line = { published("v1"), published("v2", "v1"), published("v3", "v2") };

		assertThat(pending("v1", Set.of(), line)).contains(Set.of("v3"));
		assertThat(pending("v2", Set.of(), line)).contains(Set.of("v3"));
		assertThat(pending("v3", Set.of(), line)).contains(Set.of());
	}

	@Test
	void eachBranchGetsItsOwnLatestUpdate() {
		Node[] branches = { published("v1"), published("v2", "v1"), published("v3", "v2"),
				published("v3-ca", "v2") };

		assertThat(pending("v1", Set.of(), branches)).contains(Set.of("v3", "v3-ca"));
		// a file on one branch is not behind the other: v3-ca does not descend from v3
		assertThat(pending("v3", Set.of(), branches)).contains(Set.of());
	}

	@Test
	void aDeclinedVersionIsNotOfferedButALaterOneThatContainsItIs() {
		Node[] line = { published("v4"), published("v5", "v4"), published("v7", "v5") };

		assertThat(pending("v4", Set.of("v5"), line)).contains(Set.of("v7"));
	}

	@Test
	void decliningTheLatestResurfacesTheEarlierOne() {
		Node[] line = { published("v4"), published("v5", "v4"), published("v6", "v5"), published("v7", "v6") };

		assertThat(pending("v4", Set.of("v7"), line)).contains(Set.of("v6"));
		assertThat(pending("v4", Set.of("v5", "v6", "v7"), line)).contains(Set.of());
	}

	@Test
	void withdrawingTheLatestReExposesTheOneItWasHiding() {
		assertThat(pending("v1", Set.of(), published("v1"), published("v6", "v1"), published("v7", "v6")))
				.contains(Set.of("v7"));
		assertThat(pending("v1", Set.of(), published("v1"), published("v6", "v1"), withdrawn("v7", "v6")))
				.contains(Set.of("v6"));
	}

	@Test
	void aWithdrawnVersionStillCarriesLineage() {
		Optional<Set<String>> result = pending("v1", Set.of(), published("v1"), withdrawn("v2", "v1"),
				published("v3", "v2"));

		assertThat(result).contains(Set.of("v3"));
	}

	@Test
	void aMergeVersionWithTwoParentsIsReachableFromBoth() {
		Node[] merge = { published("v1"), published("v2", "v1"), published("v3", "v1"), published("v4", "v2", "v3") };

		assertThat(pending("v2", Set.of(), merge)).contains(Set.of("v4"));
		assertThat(pending("v3", Set.of(), merge)).contains(Set.of("v4"));
		assertThat(pending("v1", Set.of(), merge)).contains(Set.of("v4"));
	}

	@Test
	void aBaselineUnknownToTheGraphIsNeverReportedAsUpToDate() {
		assertThat(pending("v-nowhere", Set.of(), published("v1"), published("v2", "v1"))).isEmpty();
	}

	@Test
	void theSameInputsAreComputedOnce() {
		PendingUpdateCalculator calculator = new PendingUpdateCalculator(
				new VersionGraph(1, 1, java.util.List.of(published("v1"), published("v2", "v1"))));

		assertThat(calculator.pendingFor("v1", Set.of())).isSameAs(calculator.pendingFor("v1", Set.of()));
	}
}
