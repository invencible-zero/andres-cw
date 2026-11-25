package com.invencible.labs.cw.fanout.model;

/**
 * The idempotency key of a unit of downstream work: {@code (engagementFileId, operation, targetVersion,
 * lineageRevision)} (ADD section 5). Two tasks with the same key are the same work, whoever enqueued them.
 */
public record TaskKey(long fileId, Operation operation, String targetVersion, long lineageRevision) {

	/** Only verification loads exist today. Applying an update is out of scope for this assignment. */
	public enum Operation { LOAD_VERIFY }

	/** Also the value sent to the downstream as its idempotency key. */
	@Override
	public String toString() {
		return fileId + ":" + operation + ":" + targetVersion + ":r" + lineageRevision;
	}
}
