# Fan-out worker: code guide

This package is the template-publish fan-out worker of [ADD section 5](../../../../../../../../documentation/design/ADD.md). It does two jobs. On a publish or withdrawal event it recomputes every affected engagement file's pending updates (fast, no downstream call). For the files it cannot trust, it queues slow loads and spends the downstream's capacity on them without hurting it.

The design reasoning and tradeoffs are in the [root README](../../../../../../../../README.md). This guide answers a different question: **what is each layer for, and what is each class for.**

## The layers

```
        config ───────────────► engine ───────────────► port ───────────────► model
   (wires and hosts)         (behaviour)             (contracts)             (the language)
            │                                              ▲
            └──────────────► sim ──────────────────────────┘
                       (stand-ins for the outside world)
```

| Layer | Package | Responsibility | May depend on | Framework |
| -- | -- | -- | -- | -- |
| **model** | `fanout.model` | The vocabulary of the problem and its pure rules. No I/O, no threads, no clock. | nothing | none |
| **port** | `fanout.port` | The contracts with everything outside the worker: the slow downstream, the Update Index, the Product Engine, the durable queue. Interfaces and their small value types only. | model | none |
| **engine** | `fanout.engine` | All behaviour: the fast path, the scheduler that protects the downstream, the resilience controls, the reference task store. | model, port | none (SLF4J only) |
| **sim** | `fanout.sim` | In-memory and simulated implementations of the ports, used by the demo and the tests. They are the stand-ins for DynamoDB, the Product Engine API and the other team's loading service. | model, port | none |
| **config** | `fanout.config` | Spring wiring, the `fanout.*` settings, and the demo that plays a publish end to end. The only layer that knows about Spring. | everything | Spring Boot |

Two rules hold the layers together. Dependencies only point inward, towards `model`. And `engine` never imports `sim`, so the real adapters of a later iteration replace the simulated ones without touching any behaviour. Both are true today (checked by scanning the imports), but nothing enforces them yet. An architecture test would be the next step.

## How the pieces run

**A publish (fast path, seconds).** An event reaches `FanOutWorker.handle`. It reads the current graph through `VersionGraphSource`, scans the template's files through `EngagementIndex` in parallel shards, asks `PendingUpdateCalculator` for each file's pending updates, and writes them back with conditional writes. A file it cannot evaluate becomes a `LoadTask` in the `TaskStore`.

**A load (slow path, hours).** `LoadScheduler` pulls tasks from the `TaskStore`. A call starts only when the `CircuitBreaker`, the `AimdLimiter` and the `TokenBucket` all agree. It then runs `FileLoader.load` on a virtual thread. On success it hands the result back to `FanOutWorker.onLoaded`, which verifies the file's baseline and recomputes that one file.

## model: the language of the problem

- **`TemplateVersionEvent`**: the event contract from the Integration Domain (published or withdrawn). It carries identifiers only, and it may arrive twice, late or out of order, so the worker never trusts it for anything but "something changed in this template".
- **`VerificationState`**: `VERIFIED` or `UNKNOWN`. Whether the index can trust a file's baseline. It is the root of the honest indicator: an `UNKNOWN` file is never shown as up to date.
- **`EngagementFile`**: one row of the Update Index as the worker sees it (baseline, state, declined versions). Its `rowVersion` changes whenever an input of the pending computation changes, which is what makes conditional writes possible.
- **`VersionGraph`**: an immutable snapshot of a template's versions (parents, market, published or withdrawn) stamped with the lineage revision it was read at. It answers "what are the ancestors of this version", walking through withdrawn versions because they still carry lineage.
- **`PendingUpdateCalculator`**: the pure function `(baseline, declined, graph) -> pending versions`. Updates collapse into the latest per branch, a decline pins only its own version, and a baseline unknown to the graph yields "cannot evaluate", never "up to date". Results are memoised per graph snapshot.
- **`TaskKey`**: the idempotency key `(file, operation, targetVersion, lineageRevision)`. Two tasks with the same key are the same work. It is also sent to the downstream as its idempotency key.
- **`LoadTask`**: one queued unit of work: a key, plus the firm (for fairness) and the template.
- **`LoadRequest`**: what the downstream receives: the file, the idempotency key and the deadline the caller will wait.
- **`LoadResult`**: what a load reveals: the baseline the file is really on, never any content.
- **`LoadException`**: the only way a `FileLoader` reports a failure. Its `Kind` (`THROTTLED`, `TIMEOUT`, `UNAVAILABLE`, `REJECTED`) is the whole contract, because it decides how the scheduler reacts.

## port: the contracts

- **`FileLoader`**: the slow downstream owned by another team. It blocks, must enforce the deadline it is given, must be safe to call twice with the same key, reports failures only as `LoadException`, and honours interruption.
- **`EngagementIndex`**: the regional Update Index: scan a template's files by shard and cursor, read one file, write pending rows conditionally (`replacePending`), record a verified baseline only while the file is still `UNKNOWN` (`markVerified`), and demote files to `UNKNOWN`. Its nested `Page` and `PendingWrite` are the value types of those calls.
- **`VersionGraphSource`**: reads a template's current `VersionGraph` through the Product Engine API.
- **`TaskStore`**: the durable queue and the idempotency ledger in one. Enqueue is idempotent per key, `claimNext` applies fairness, the per-firm cap and one-task-per-file, and every claim is a lease with a fencing id. It also records failures, deferrals, dead letters and their replay. Its nested types are `Claim`, `DeadLetter`, `Counts` and the result enums.

## engine: the behaviour

- **`FanOutWorker`**: the heart of the fast path. `handle(event)` recomputes the whole template and enqueues loads, and it is safe to replay. It also implements `LoadTaskHandler`, so the same recompute runs for one file after a successful load (the self-healing path).
- **`LoadTaskHandler`**: the two callbacks the scheduler needs from the business side ("is this load still needed?" and "apply this result"). It keeps `LoadScheduler` generic.
- **`LoadScheduler`**: protects the downstream. One dispatcher thread decides whether a call may start, a virtual thread per call does the blocking work, and each outcome is turned into the right reaction (retry, defer, dead-letter, slow down). It also owns graceful shutdown, which returns in-flight tasks to the queue.
- **`InMemoryTaskStore`**: the reference implementation of `TaskStore` and the executable specification of its state machine. It also holds the fairness rule (the firm with the fewest tasks in flight goes next) and the lease and fencing logic.
- **`AimdLimiter`**: the adaptive concurrency limit. It grows slowly while the limit is in use, halves on a 429 and trims on a timeout, and counts a burst of signals once.
- **`TokenBucket`**: caps the rate at which calls start, so that a full limit does not fire at once. A downstream `Retry-After` pauses it for everyone.
- **`CircuitBreaker`**: stops calling a downstream that is down and sends a single probe to find out when it is back. Its permits are bound to the state they were issued in, so a stale result cannot flip it.
- **`Backoff`**: retry delay, exponential with a cap and "equal jitter", so that a failed wave does not retry in lockstep.
- **`FanOutStats`**: the counters behind the SLOs (events, loads, throttles, retries, dead letters). It has no framework dependency, so a metrics binding can read it later.

## sim: stand-ins for the outside world

- **`InMemoryEngagementIndex`**: an in-memory Update Index that honours the conditional-write contract of the port. It adds read helpers (`statusOf`, `pendingOf`, `statusCounts`) and the `Status` enum that the demo and the tests use to read the indicator.
- **`InMemoryVersionGraphSource`**: a mutable catalog standing in for the Product Engine. `publish` and `withdraw` change the graph and return the new lineage revision to put in an event.
- **`SimulatedFileLoader`**: the other team's loading service, simulated. It has latency, a hard capacity that answers 429 beyond it, calls that hang until the deadline, 5xx errors, an outage switch, scripted failures per file and files that can never load. It keeps the books the demo and the tests check: calls per file and the peak concurrency it ever accepted.

## config: wiring and the demo

- **`FanOutProperties`**: every `fanout.*` knob, with defaults sized for the real one-minute load. The concurrency numbers are the ones to agree with the team that owns the load operation.
- **`FanOutConfiguration`**: builds the engine from the properties and connects it to the port beans. Active only with `fanout.enabled=true`, and it starts and stops the scheduler with the Spring context.
- **`DemoProperties`**: the size of the simulated world (how many firms and files) and how the downstream misbehaves.
- **`DemoConfiguration`**: active only in the `demo` profile. It registers the `sim` adapters, which is what makes `FanOutConfiguration` complete.
- **`DemoRunner`**: the demo itself. It plays a publish, a replayed event, a drain under failure and a withdrawal against the simulated world, prints a report, and exits.

## Outside this package

- **`AndresCaseApplication`**: the plain Spring Boot entry point. Component scanning picks up `fanout.config` from here.
- **`src/main/resources/application.properties`**: the single settings file. The `fanout.*` demo values live in its `demo` profile section, so the default application is unaffected.
- **`db/changelog/`**: the Liquibase baseline. The fan-out worker does not use the database yet.

## Tests

| Test | What it covers |
| -- | -- |
| `PendingUpdateCalculatorTest` | The scenarios of the case: branches, withdrawals, "declined v5, later offered v7". |
| `AimdLimiterTest`, `TokenBucketTest`, `CircuitBreakerTest`, `BackoffTest` | Each control in isolation, with a manual clock so the timing rules are deterministic. |
| `InMemoryTaskStoreTest` | Idempotent enqueue, fairness, caps, one task per file, retries, dead letters, leases and fencing. |
| `FanOutWorkerTest` | The fast path, replays, and the conditional writes of the index. |
| `LoadSchedulerTest` | The whole worker with real threads against the simulated downstream: capacity never exceeded, nothing loaded twice, throttling, outage, shutdown. |
| `DemoWiringTest` | The real Spring wiring, by running the demo with a tiny world. |
| `MutableClock`, `SchedulerRig` | Test helpers: a clock that moves on command, and the worker wired with short durations. |
