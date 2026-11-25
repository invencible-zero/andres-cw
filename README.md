# andres-cw

Spring Boot 4.2.0 baseline on Java 25 (Maven).

## Run

```
./mvnw spring-boot:run
```

On Windows PowerShell use `.\mvnw.cmd spring-boot:run`. The app listens on port 8080.

Health check: <http://localhost:8080/actuator/health>

## Database

In-memory H2 (`jdbc:h2:mem:andres_cw`), recreated on every start. Liquibase builds the schema on startup, so no manual SQL is needed.

- Master changelog: `src/main/resources/db/changelog/db.changelog-master.yaml`
- Add a schema change: create `db/changelog/changes/NNN-description.yaml` and `include` it in the master file. Never edit a changeset that has already run; add a new one.
- Applied changesets are recorded in the `DATABASECHANGELOG` table.
- Datasource and Liquibase settings are in `application.properties`.
- H2 web console: <http://localhost:8080/h2-console> (JDBC URL `jdbc:h2:mem:andres_cw`, user `sa`, empty password). It only works while the app is running, and it is limited to local access. Disable it (`spring.h2.console.enabled=false`) in any deployed environment.
- Because H2 is in-memory, data is lost on restart. Swapping to a persistent database means changing `spring.datasource.*` and the driver dependency; the changelogs stay as they are.

## Build and test

```
./mvnw verify
java -jar target/andres-cw-0.0.1-SNAPSHOT.jar
```

## Part 2: the template-publish fan-out worker

The component of [ADD section 5](documentation/design/ADD.md): on a publish or a withdrawal it updates every affected engagement file's indicator, and it spends a scarce, slow downstream (a ~1 minute load owned by another team) without hurting it. Iteration 1 is a framework-free engine behind four ports (`FileLoader`, `EngagementIndex`, `VersionGraphSource`, `TaskStore`), a thin Spring host, and in-memory or simulated adapters. There is no Kafka, DynamoDB or AWS yet.

```
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=demo"    # ~25 s: one publish against a simulated world
.\mvnw.cmd verify                                              # 66 tests
```

For what each layer and class is for, see the [code guide](src/main/java/com/invencible/labs/cw/fanout/README.md).

The demo compresses a 60 s load to 400 ms. It publishes v3 to 1,350 files in 56 firms (one giant), replays the event, drains 556 load tasks against a downstream that throttles, times out, goes down for 2 s and has 8 corrupt files, then withdraws v3. Expect the fast path in tens of milliseconds, 0 duplicate loads, about 10 wasted calls during the outage, small firms done in seconds while the giant firm finishes last, and `UNKNOWN` equal to exactly the 8 files that could not be verified.

```
event (Kafka, at-least-once) ──► FanOutWorker.handle                      FAST PATH: seconds, no downstream call
                                   read current version graph · scan the template in 8 shards (hash of file id)
                                   recompute pending updates (pure function) · conditional batch writes to the index
                                   UNKNOWN or unresolvable files ──► enqueue a load task (idempotent)
                                                  │
                                         TaskStore (queue + ledger): firm with the fewest tasks in flight goes next
                                                  │
                                         LoadScheduler                    SLOW PATH: hours, protects the downstream
                                           a call starts only if breaker AND AIMD limit AND token bucket allow it
                                           one virtual thread per call ──► FileLoader.load()
                                                  │ success
                                         FanOutWorker.onLoaded: verify the baseline, recompute that one file
```

### Key tradeoffs

| Decision | Why | What it costs |
| -- | -- | -- |
| **Two paths.** The publish never waits for a load. | The indicator is up in seconds. Loads only verify files the index cannot trust. | A file reads `UNKNOWN` until it is verified. That is deliberate: never "up to date" for an unverified file. |
| **Three guards for three failure modes.** AIMD concurrency limit, token bucket, circuit breaker. | The downstream's capacity is unknown and moves, so the limit is discovered (429 halves it, a timeout only trims it by 10%, because a timeout is ambiguous). The bucket smooths bursts and honours `Retry-After`. The breaker stops calling something that is down. | Three knobs to agree with the owning team. AIMD state is per instance and resets on restart (slow start again). |
| **Idempotency in layers, not exactly-once.** Ledger key `(file, op, targetVersion, lineageRevision)`, one task per file at a time, check-before-act, conditional index writes. | A replayed event enqueues nothing. A file a user opened meanwhile is not loaded (a minute saved). A load result is dropped if something newer verified the file. A write from stale inputs is refused (`rowVersion`, strictly newer revision). | A lease expiry or an ambiguous timeout can cause one duplicate load. It costs capacity, never correctness, because a load only reads. |
| **Fairness by least-in-flight-first, plus a hard per-firm cap.** | No weights, work-conserving, and the 40,000-file firm cannot starve a 40-file firm. The cap protects that firm's own store and pods. | A firm that is alone is capped. At 25 the giant firm needs ~26 h, which still fits inside the 5.6-day backfill because other firms fill the other slots. |
| **Failures are classified, not just retried.** | `REJECTED` is dead-lettered at once. `THROTTLED` is not the task's fault, so it spends no attempt. `TIMEOUT` and `UNAVAILABLE` retry with backoff and jitter, then dead-letter. | The classification is a contract the real loader adapter must honour. |
| **Crash safety by lease and fencing id.** | A claim is a lease. An expired one returns to the queue and counts as an attempt, so a task that kills workers ends up dead. A worker that lost its lease cannot complete the task. | The lease must outlive the call deadline (3 min vs 90 s). |
| **Virtual threads.** | The limit is a semantic limit, not a pool size. A hundred blocked one-minute calls are cheap. | None worth noting on Java 25. |
| **In-memory store, index and graph.** | `InMemoryTaskStore` is the executable specification of the state machine. The ports are the contract. | Production maps each transition to a DynamoDB conditional write, and the ledger needs a TTL of at least the Kafka retention (7 days). |

Re-delivering an event is also the recovery path: `handle` reads the current graph, never the payload, so a crash half-way is repaired by a replay and the scan needs no checkpoint.

### Not in iteration 1

- **Several instances.** Each has its own limiter, so N instances use N times the budget. Next: a shared bucket and limit in Valkey (already in the design), or a static split of the budget.
- **Load result durability.** If the index write fails after a successful load, the load is repeated (a minute wasted). Persisting the result first would fix it.
- **Adapters and drivers:** Kafka consumer, DynamoDB index and ledger, the Product Engine client, and the backfill driver (it reuses `TaskStore.enqueue`).
- **Priorities** (publish before backfill), Micrometer binding for `FanOutStats`, and short-circuiting events older than the stored revision (harmless today, only redundant).
