package com.invencible.labs.cw.fanout.port;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.invencible.labs.cw.fanout.model.EngagementFile;

/**
 * The regional Update Index as the worker uses it (ADD section 2.2). Every method is idempotent, and writes of
 * derived rows are conditional, so retries and concurrent workers cannot corrupt it.
 */
public interface EngagementIndex {

	/** A page of files. {@code nextCursor} is opaque and null on the last page, so a scan can resume. */
	record Page(List<EngagementFile> files, String nextCursor) {
	}

	/**
	 * One derived row set for a file.
	 *
	 * @param expectedRowVersion the {@link EngagementFile#rowVersion()} the result was computed from
	 * @param lineageRevision    the graph revision it was computed at
	 */
	record PendingWrite(long fileId, long templateId, Set<String> targets, long expectedRowVersion,
			long lineageRevision, Instant computedAt) {
	}

	/**
	 * Files of a template, restricted to one shard. The shard is a hash of the file id, never of the firm, so the
	 * 40,000-file firm spreads over all shards instead of landing on one.
	 */
	Page scan(long templateId, int shard, int shards, String cursor, int limit);

	Optional<EngagementFile> get(long fileId);

	/**
	 * Replaces the pending rows of each file, but only where the file's {@code rowVersion} still equals the expected
	 * one (its inputs did not change since) and the revision is strictly newer than the stored one. The result is a pure
	 * function of the inputs and the revision, so a replay of the same revision is a no-op, and that saves the write.
	 * A rejected write is not an error: whoever changed the inputs recomputes on their own, and the newer revision wins.
	 *
	 * @return how many writes were applied
	 */
	int replacePending(List<PendingWrite> writes);

	/**
	 * Records a verified baseline (a load succeeded), but only while the file is still UNKNOWN. A load takes a minute,
	 * and if anything verified the file meanwhile (a user opened it, an apply hook ran), that information is newer than
	 * this result, so the result is dropped instead of regressing the baseline.
	 *
	 * @return the new row (its version bumped), or empty when the result was dropped or the file does not exist
	 */
	Optional<EngagementFile> markVerified(long fileId, String baselineVersion, Instant verifiedAt);

	/** Honest indicator: files that cannot be evaluated are UNKNOWN, with no pending rows. */
	void markUnknown(Collection<Long> fileIds);
}
