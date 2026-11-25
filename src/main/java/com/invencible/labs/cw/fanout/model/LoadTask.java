package com.invencible.labs.cw.fanout.model;

/** A queued request to load one engagement file. The firm is carried for fairness and per-firm caps. */
public record LoadTask(TaskKey key, long firmId, long templateId) {

	public long fileId() {
		return key.fileId();
	}
}
