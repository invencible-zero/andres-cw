package com.invencible.labs.cw.fanout.sim;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import com.invencible.labs.cw.fanout.model.EngagementFile;
import com.invencible.labs.cw.fanout.model.VerificationState;
import com.invencible.labs.cw.fanout.port.EngagementIndex;

/**
 * A stand-in for the regional Update Index (DynamoDB in the design). It implements the conditional-write contract of
 * the port faithfully, because that contract is what keeps concurrent workers correct. Rows are guarded by their own
 * monitor, which a real table replaces with a conditional expression on {@code rowVersion}.
 */
public final class InMemoryEngagementIndex implements EngagementIndex {

	/** The indicator users see (ADD section 3). */
	public enum Status { UP_TO_DATE, UPDATES_PENDING, UNKNOWN }

	private static final class Row {
		final long id;
		final long firmId;
		final long templateId;
		String baseline;
		VerificationState state;
		Set<String> declined;
		long version;
		Set<String> pending = Set.of();
		boolean pendingComputed;
		long pendingRevision = -1;

		Row(long id, long firmId, long templateId, String baseline, VerificationState state, Set<String> declined) {
			this.id = id;
			this.firmId = firmId;
			this.templateId = templateId;
			this.baseline = baseline;
			this.state = state;
			this.declined = declined;
		}
	}

	private final ConcurrentSkipListMap<Long, Row> rows = new ConcurrentSkipListMap<>();

	public void add(long id, long firmId, long templateId, String baseline, VerificationState state,
			Set<String> declined) {
		rows.put(id, new Row(id, firmId, templateId, baseline, state, Set.copyOf(declined)));
	}

	@Override
	public Page scan(long templateId, int shard, int shards, String cursor, int limit) {
		long after = cursor == null ? Long.MIN_VALUE : Long.parseLong(cursor);
		List<EngagementFile> files = new ArrayList<>(limit);
		for (Row row : rows.tailMap(after, false).values()) {
			if (row.templateId == templateId && Math.floorMod(Long.hashCode(row.id), shards) == shard) {
				files.add(snapshot(row));
				if (files.size() == limit) {
					break;
				}
			}
		}
		String next = files.size() == limit ? String.valueOf(files.get(files.size() - 1).id()) : null;
		return new Page(files, next);
	}

	@Override
	public Optional<EngagementFile> get(long fileId) {
		return Optional.ofNullable(rows.get(fileId)).map(InMemoryEngagementIndex::snapshot);
	}

	@Override
	public int replacePending(List<PendingWrite> writes) {
		int applied = 0;
		for (PendingWrite write : writes) {
			Row row = rows.get(write.fileId());
			if (row == null) {
				continue;
			}
			synchronized (row) {
				boolean inputsUnchanged = row.version == write.expectedRowVersion();
				boolean newer = write.lineageRevision() > row.pendingRevision;
				if (inputsUnchanged && newer && row.state == VerificationState.VERIFIED) {
					row.pending = Set.copyOf(write.targets());
					row.pendingComputed = true;
					row.pendingRevision = write.lineageRevision();
					applied++;
				}
			}
		}
		return applied;
	}

	@Override
	public Optional<EngagementFile> markVerified(long fileId, String baselineVersion, Instant verifiedAt) {
		Row row = rows.get(fileId);
		if (row == null) {
			return Optional.empty();
		}
		synchronized (row) {
			if (row.state != VerificationState.UNKNOWN) {
				return Optional.empty();
			}
			row.baseline = baselineVersion;
			row.state = VerificationState.VERIFIED;
			resetDerived(row);
			return Optional.of(snapshot(row));
		}
	}

	@Override
	public void markUnknown(Collection<Long> fileIds) {
		for (long id : fileIds) {
			Row row = rows.get(id);
			if (row != null) {
				synchronized (row) {
					row.state = VerificationState.UNKNOWN;
					resetDerived(row);
				}
			}
		}
	}

	/** Inputs changed: the derived rows are void, and a write computed from the old inputs must be refused. */
	private static void resetDerived(Row row) {
		row.version++;
		row.pending = Set.of();
		row.pendingComputed = false;
		row.pendingRevision = -1;
	}

	private static EngagementFile snapshot(Row row) {
		synchronized (row) {
			return new EngagementFile(row.id, row.firmId, row.templateId, row.baseline, row.state, row.declined,
					row.version);
		}
	}

	// ---------------------------------------------------------------- reading the result (demo and tests)

	/** UNKNOWN covers both "not verified" and "verified but not yet evaluated". Never "up to date" by default. */
	public Status statusOf(long fileId) {
		Row row = rows.get(fileId);
		synchronized (row) {
			if (row.state == VerificationState.UNKNOWN || !row.pendingComputed) {
				return Status.UNKNOWN;
			}
			return row.pending.isEmpty() ? Status.UP_TO_DATE : Status.UPDATES_PENDING;
		}
	}

	public Set<String> pendingOf(long fileId) {
		Row row = rows.get(fileId);
		synchronized (row) {
			return row.pending;
		}
	}

	public Map<Status, Long> statusCounts() {
		return rows.keySet().stream().collect(Collectors.groupingBy(this::statusOf, Collectors.counting()));
	}

	public Map<Long, Long> unknownCountByFirm() {
		return rows.values().stream().filter(r -> statusOf(r.id) == Status.UNKNOWN)
				.collect(Collectors.groupingBy(r -> r.firmId, Collectors.counting()));
	}

	public int size() {
		return rows.size();
	}
}
