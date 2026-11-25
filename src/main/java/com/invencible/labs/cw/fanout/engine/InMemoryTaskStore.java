package com.invencible.labs.cw.fanout.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import com.invencible.labs.cw.fanout.model.LoadTask;
import com.invencible.labs.cw.fanout.model.TaskKey;
import com.invencible.labs.cw.fanout.port.TaskStore;

/**
 * Reference implementation of {@link TaskStore}: one lock, plain collections. It is the specification of the
 * transitions that a durable implementation has to reproduce with conditional writes.
 *
 * <pre>
 *   QUEUED --claim--> IN_FLIGHT --complete--> DONE
 *      ^                 |  |--fail (attempts left)--> QUEUED (after backoff)
 *      |                 |  |--fail (exhausted) / deadLetter--> DEAD --requeueDead--> QUEUED
 *      +--defer / lease expired
 * </pre>
 *
 * <p>Fairness: among firms that have a runnable task and are under their cap, the one with the fewest tasks in flight
 * is served next (ties go to the one served least recently). That is max-min fairness. It needs no weights, it is
 * work-conserving when only one firm has work, and it self-corrects as tasks finish: a firm that arrives while the
 * 40,000-file firm is running gets slots as soon as the next task completes.
 *
 * <p>The per-firm cap is a separate, hard limit. Its job is to protect one customer's own database and pods from
 * the whole concurrency budget, not to provide fairness.
 *
 * <p>Known limit: finished entries stay in the ledger forever. A durable store expires them (TTL), and the TTL must
 * be at least the event retention (7 days), or a replayed event would re-run finished work.
 */
public final class InMemoryTaskStore implements TaskStore {

	private enum State { QUEUED, IN_FLIGHT, DONE, DEAD }

	private static final class Entry {
		final LoadTask task;
		final long seq;
		State state = State.QUEUED;
		int attempts;
		Instant notBefore;
		long claimId;
		Instant leaseUntil;

		Entry(LoadTask task, long seq, Instant notBefore) {
			this.task = task;
			this.seq = seq;
			this.notBefore = notBefore;
		}
	}

	private static final class Firm {
		/** Holds exactly the QUEUED entries. An entry must leave it before its notBefore changes. */
		final TreeSet<Entry> queue = new TreeSet<>(
				Comparator.comparing((Entry e) -> e.notBefore).thenComparingLong(e -> e.seq));
		int inFlight;
		long lastServed;
	}

	private final int maxInFlightPerFirm;
	private final int maxAttempts;
	private final Duration lease;
	private final Clock clock;

	private final Map<TaskKey, Entry> ledger = new HashMap<>();
	private final Map<Long, Firm> firms = new HashMap<>();
	private final Set<Long> filesInFlight = new HashSet<>();
	private final Set<Entry> leased = new HashSet<>();
	private final Map<TaskKey, DeadLetter> dead = new LinkedHashMap<>();
	private long seq;
	private long claimSeq;
	private long serveSeq;
	private long done;

	/**
	 * @param lease must be longer than the call deadline, so a healthy call never loses its lease
	 */
	public InMemoryTaskStore(int maxInFlightPerFirm, int maxAttempts, Duration lease, Clock clock) {
		this.maxInFlightPerFirm = maxInFlightPerFirm;
		this.maxAttempts = maxAttempts;
		this.lease = lease;
		this.clock = clock;
	}

	@Override
	public synchronized EnqueueResult enqueue(LoadTask task) {
		if (ledger.containsKey(task.key())) {
			return EnqueueResult.DUPLICATE;
		}
		Entry entry = new Entry(task, ++seq, clock.instant());
		ledger.put(task.key(), entry);
		firms.computeIfAbsent(task.firmId(), id -> new Firm()).queue.add(entry);
		return EnqueueResult.ENQUEUED;
	}

	@Override
	public synchronized Optional<Claim> claimNext() {
		Instant now = clock.instant();
		Firm bestFirm = null;
		Entry bestEntry = null;
		for (Firm firm : firms.values()) {
			if (firm.inFlight >= maxInFlightPerFirm) {
				continue;
			}
			Entry candidate = firstRunnable(firm, now);
			if (candidate != null && (bestFirm == null || serveBefore(firm, candidate, bestFirm, bestEntry))) {
				bestFirm = firm;
				bestEntry = candidate;
			}
		}
		if (bestEntry == null) {
			return Optional.empty();
		}
		bestFirm.queue.remove(bestEntry);
		bestFirm.inFlight++;
		bestFirm.lastServed = ++serveSeq;
		bestEntry.state = State.IN_FLIGHT;
		bestEntry.claimId = ++claimSeq;
		bestEntry.leaseUntil = now.plus(lease);
		filesInFlight.add(bestEntry.task.fileId());
		leased.add(bestEntry);
		return Optional.of(new Claim(bestEntry.task, bestEntry.claimId, bestEntry.attempts));
	}

	private static boolean serveBefore(Firm a, Entry aHead, Firm b, Entry bHead) {
		if (a.inFlight != b.inFlight) {
			return a.inFlight < b.inFlight;
		}
		if (a.lastServed != b.lastServed) {
			return a.lastServed < b.lastServed;
		}
		return aHead.seq < bHead.seq;
	}

	/** The oldest due task whose file has no other task in flight. Never two concurrent loads of one file. */
	private Entry firstRunnable(Firm firm, Instant now) {
		for (Entry entry : firm.queue) {
			if (entry.notBefore.isAfter(now)) {
				return null;
			}
			if (!filesInFlight.contains(entry.task.fileId())) {
				return entry;
			}
		}
		return null;
	}

	@Override
	public synchronized boolean complete(Claim claim) {
		Entry entry = holder(claim);
		if (entry == null) {
			return false;
		}
		unlease(entry);
		entry.state = State.DONE;
		done++;
		return true;
	}

	@Override
	public synchronized boolean defer(Claim claim, Instant notBefore) {
		Entry entry = holder(claim);
		if (entry == null) {
			return false;
		}
		unlease(entry);
		requeue(entry, notBefore);
		return true;
	}

	@Override
	public synchronized FailResult fail(Claim claim, Instant notBefore, String reason) {
		Entry entry = holder(claim);
		return entry == null ? FailResult.STALE : failEntry(entry, notBefore, reason);
	}

	@Override
	public synchronized boolean deadLetter(Claim claim, String reason) {
		Entry entry = holder(claim);
		if (entry == null) {
			return false;
		}
		unlease(entry);
		entry.attempts++;
		kill(entry, reason);
		return true;
	}

	@Override
	public synchronized int reclaimExpired() {
		Instant now = clock.instant();
		List<Entry> expired = leased.stream().filter(e -> e.leaseUntil.isBefore(now)).toList();
		expired.forEach(e -> failEntry(e, now, "lease expired"));
		return expired.size();
	}

	@Override
	public synchronized List<DeadLetter> deadLetters() {
		return List.copyOf(dead.values());
	}

	@Override
	public synchronized boolean requeueDead(TaskKey key) {
		Entry entry = ledger.get(key);
		if (entry == null || entry.state != State.DEAD) {
			return false;
		}
		dead.remove(key);
		entry.attempts = 0;
		requeue(entry, clock.instant());
		return true;
	}

	@Override
	public synchronized Counts counts() {
		int queued = firms.values().stream().mapToInt(f -> f.queue.size()).sum();
		return new Counts(queued, leased.size(), done, dead.size());
	}

	/** The entry, if this claim still holds its lease. A stale claim (fencing) gets null and must be ignored. */
	private Entry holder(Claim claim) {
		Entry entry = ledger.get(claim.task().key());
		boolean holds = entry != null && entry.state == State.IN_FLIGHT && entry.claimId == claim.claimId();
		return holds ? entry : null;
	}

	private FailResult failEntry(Entry entry, Instant notBefore, String reason) {
		unlease(entry);
		entry.attempts++;
		if (entry.attempts >= maxAttempts) {
			kill(entry, reason);
			return FailResult.DEAD;
		}
		requeue(entry, notBefore);
		return FailResult.RETRY;
	}

	private void unlease(Entry entry) {
		leased.remove(entry);
		filesInFlight.remove(entry.task.fileId());
		firms.get(entry.task.firmId()).inFlight--;
	}

	private void requeue(Entry entry, Instant notBefore) {
		entry.state = State.QUEUED;
		entry.notBefore = notBefore;
		firms.get(entry.task.firmId()).queue.add(entry);
	}

	private void kill(Entry entry, String reason) {
		entry.state = State.DEAD;
		dead.put(entry.task.key(), new DeadLetter(entry.task, entry.attempts, reason, clock.instant()));
	}
}
