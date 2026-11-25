package com.invencible.labs.cw.fanout.engine;

import com.invencible.labs.cw.fanout.model.LoadResult;
import com.invencible.labs.cw.fanout.model.LoadTask;

/**
 * What the scheduler needs to know about the work, so that it can stay a generic protector of the downstream.
 * {@link FanOutWorker} implements it.
 */
public interface LoadTaskHandler {

	/**
	 * Checked right before the call. A load costs a minute of someone else's capacity, and the world moves while a task
	 * waits: a user may have opened the file and healed the index, or another task for the same file may have
	 * finished. When this returns false the call is skipped.
	 */
	boolean isStillNeeded(LoadTask task);

	/** Applies a successful load. Must be idempotent: it can run again after a lease expiry. */
	void onLoaded(LoadTask task, LoadResult result);
}
