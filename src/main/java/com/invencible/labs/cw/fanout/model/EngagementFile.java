package com.invencible.labs.cw.fanout.model;

import java.util.Set;

/**
 * A row of the Update Index, as the fan-out worker sees it. The engagement file stays the source of truth for its
 * baseline, and this row is derived data.
 *
 * @param baselineVersion  the creation version or last applied update. For an {@link VerificationState#UNKNOWN} file
 *                         this is only the best-known value.
 * @param declinedVersions versions the firm declined for this file (decisions are append-only)
 * @param rowVersion       bumped by every change to an input of the pending computation (baseline, verification,
 *                         decisions). Writes of derived rows are conditional on it, so a result computed from stale
 *                         inputs can never overwrite a fresher one.
 */
public record EngagementFile(
		long id,
		long firmId,
		long templateId,
		String baselineVersion,
		VerificationState state,
		Set<String> declinedVersions,
		long rowVersion) {

	public EngagementFile {
		declinedVersions = Set.copyOf(declinedVersions);
	}
}
