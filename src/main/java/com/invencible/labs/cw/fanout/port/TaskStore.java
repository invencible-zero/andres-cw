package com.invencible.labs.cw.fanout.port;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.invencible.labs.cw.fanout.model.LoadTask;
import com.invencible.labs.cw.fanout.model.TaskKey;

/**
 * The durable queue and the idempotency ledger in one place. A task is known by its {@link TaskKey}, and the key stays
 * in the ledger after the task finishes, so a replayed event cannot re-run finished or dead-lettered work.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #enqueue} is idempotent per key.</li>
 *   <li>{@link #claimNext} is where firm fairness lives. It never hands out two tasks for the same file at the same
 *       time, never exceeds the per-firm cap, and prefers the firm with the fewest tasks in flight.</li>
 *   <li>A claim is a lease. If it is not completed in time, {@link #reclaimExpired} puts the task back (a crashed worker
 *       loses nothing). Every claim carries a fencing id, so the result of a worker that lost its lease is ignored.</li>
 * </ul>
 *
 * <p>Iteration 1 ships an in-memory implementation. In production this is a table with a unique key on the task key
 * and conditional updates for each transition (DynamoDB), or SQS plus such a ledger.
 */
public interface TaskStore {

	enum EnqueueResult { ENQUEUED, DUPLICATE }

	enum FailResult { RETRY, DEAD, STALE }

	/** A leased task. {@code attempt} is the number of failures counted so far. */
	record Claim(LoadTask task, long claimId, int attempt) {
	}

	record DeadLetter(LoadTask task, int attempts, String reason, Instant at) {
	}

	record Counts(int queued, int inFlight, long done, int dead) {
		public boolean idle() {
			return queued == 0 && inFlight == 0;
		}
	}

	EnqueueResult enqueue(LoadTask task);

	/** Leases the next runnable task, or empty when nothing may run now. */
	Optional<Claim> claimNext();

	/** Marks the task done. Returns false when the claim is stale (lease lost). */
	boolean complete(Claim claim);

	/** Puts the task back without counting an attempt (throttling, shutdown). False when the claim is stale. */
	boolean defer(Claim claim, Instant notBefore);

	/** Counts one failed attempt: back to the queue, or to the dead-letter queue when attempts are exhausted. */
	FailResult fail(Claim claim, Instant notBefore, String reason);

	/** Permanent failure: dead-letters right away. False when the claim is stale. */
	boolean deadLetter(Claim claim, String reason);

	/** Returns expired leases to the queue, counting an attempt, so a poison task that kills workers ends up dead. */
	int reclaimExpired();

	List<DeadLetter> deadLetters();

	/** Operator action after a fix: moves a dead-lettered task back to the queue with a fresh budget. */
	boolean requeueDead(TaskKey key);

	Counts counts();
}
