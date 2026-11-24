# Architectural Design Document

## 1. Purpose and approach

This document explains the architectural approach taken for the Caseware engagement files solution. The goal is to let firms see at a glance which engagement files have a pending product-template update, and to give them a human-readable summary of the inbound changes so they can apply or decline each update. 

The problem is broken down using **Domain-Driven Design (DDD)**, which consists of identifying the business and logical domains (bounded contexts), their responsibilities and their boundaries. The following terms are used consistently across the domains and the APIs (ubiquitous language):

| Term | Meaning |
| -- | -- |
| Template version | An immutable, published snapshot of a product template. Versions form a graph (markets branch), not a line, and can be withdrawn after publication. |
| Engagement file | A firm's working file, created from a template version. |
| Baseline | The template version an engagement file is currently based on (the creation version, or the last applied update). |
| Pending update | A published, non-withdrawn version that descends from the baseline and has not been declined for that engagement file. |
| Decision | An apply or a decline recorded by a user for one engagement file and one target version. |

### Key decisions

1. **Never load engagement files to find pending updates.** A load takes ~1 minute per file. A per-region **Update Index** (a read model fed by hooks) answers "which files have a pending update" in seconds. Loading is used only for backfill, reconciliation and self-healing.
2. **Pending updates are derived, not stored as deltas.** They are a pure function of the baseline, the decisions and the version graph, so duplicated, late or out-of-order events converge to the same result.
3. **Events are notifications and delivery is at-least-once.** A transactional outbox, idempotent consumers and a periodic reconciliation cover lost or duplicated events.
4. **Summaries are generated once per version pair, from the template diff only.** They are validated, immutable and stored with their provenance, so they can be defended months later and never expose client data to the model.
5. **Engagement data lives in regional cells** (EU, Canada, default). The Product Domain is global because templates are not firm-specific.

## 2. Domains

Relationships between the domains (context map):

- **Product → Integration → Engagement:** Product is the upstream publisher. Engagement is a downstream consumer that conforms to the published event contract.
- **User → Product, Engagement:** User is an upstream service that both engines call for access decisions.
- **Financial System:** external to all domains. The Engagement Engine reaches it through an anti-corruption layer so that its model does not leak into ours.

### 2.1 Product Domain

This domain consists of two components:

- **Product Engine** microservice(s)
- **Product/Template Database** (relational)

The domain is managed by the owner of the products and templates (Caseware content teams), who create products, publish new template versions and withdraw versions when needed. Its database is the common source of truth for product and template metadata, including the version graph (parents, market, status). It holds no information about engagement files.

Responsibilities:

- Publish immutable template versions and record their lineage. A published version is never edited or deleted, only withdrawn.
- In the same database transaction that stores a new version, write an outbox record. A relay then publishes `TemplateVersionPublished` (or `TemplateVersionWithdrawn`) to the Integration Domain, so a version is never stored without its event.
- Produce the diff and the human-readable summary between two versions (section 4), as an asynchronous module of the Product Engine.
- Retrieve fine-grained permissions from the User Domain to gate user access. Internal content users can write; firm users can only read.
- Be the only entry point to template data. Other domains use its APIs and events, never its database. Today, engagement creation reads the database directly; that read is moved behind the API during migration.

The Product Engine can be implemented as a microservice, which offers horizontal scalability for high-traffic periods. An API overview is in Appendix A.

### 2.2 Engagement Domain

This is the customer-facing application. It creates new engagement files from the latest available template version, loads them with up-to-date external financial information, records apply/decline decisions and shows which files have pending updates.

It is made up of five components, four internal and one external:

- **Engagement Engine** microservice(s), including the **Update Fan-out Worker** that processes publish events and the backfill (section 5).
- **Engagement Database:** the customer-specific engagement stores plus the **Update Index**.
- **Storage System:** stores engagement files in one or multiple regions on demand.
- **Cache:** holds immutable, frequently read artifacts (template archives by version, diffs, summaries). It never caches volatile financial fields.
- **Financial System** (external): provides the volatile financial fields.

The **Update Index** holds, per region and per engagement file, the firm, product, template, baseline version, verification state, the append-only decisions and the derived pending updates. The engagement file remains the source of truth for its baseline. The index is derived data: hooks on creation, load, apply and decline update it, and it self-heals every time a file is loaded.

An API overview is in Appendix A.

### 2.3 Integration Domain

This layer is responsible for the asynchronous communication channel between domains. For this business case, publishing a template version to many firms requires a publish-subscribe approach: the Product Engine acts as the publisher and each regional Engagement Engine acts as a consumer. This asynchronous design distributes new template versions efficiently, and every consumer applies or declines the new version at its own pace.

- **Technology:** Apache Kafka (Amazon MSK). One topic for template lifecycle events, keyed by template id so that events for the same template stay ordered. Each region consumes in its own consumer group.
- **Event contract:** `{eventId, type, productId, templateId, version, parents, market, lineageRevision, occurredAt}`. Events carry identifiers, not content.
- **Delivery:** at-least-once, with a retention of at least 7 days for replay and a dead-letter queue for poison messages.
- The events contain no firm data, so they can cross regions.

### 2.4 User Domain

Through this domain, user management is carried out. Both the Product Engine and the Engagement Engine request the access grant per user for their internal functions.

Responsibilities:

- Authentication (a single `/login` for all domains) and token issuance. Engines validate signed tokens locally, so there is no synchronous call on the hot path.
- RBAC schema and entitlements (which products and markets a firm can use). Permission decisions are cached with a short TTL and fail closed.
- Residency policy: the source of truth for which region each firm belongs to. The User Domain declares the region; the Engagement Domain enforces it (section 7).

## 3. Correctness: versions, decisions and pending updates

- **Version graph.** Each template version has zero or more parents, a market and a status (published or withdrawn). v7 *contains* v5 when v5 is an ancestor of v7.
- **Pending updates.** For an engagement file with baseline *b*, the pending updates are the versions *v* that are published, descend from *b*, have not been declined for that file and are not an ancestor of another candidate. Accumulated updates therefore collapse into the latest candidate per branch, and the user makes one decision instead of several.
- **Cumulative summary.** The summary always compares the baseline with the candidate, never the previous update. If v5 was declined and v7 contains it, v7 is offered again, and its summary states "includes changes from v5, declined on *date*". The intermediate steps remain viewable.
- **What "declined" pins.** A decline is an immutable record about one engagement file and one target version: who, when and which summary was shown. It leaves the baseline unchanged and does not block later versions, including those that contain the declined changes. It does not mean "never ask again".
- **Withdrawal.** Because pending updates are derived, withdrawing v7 removes it from the lists and re-exposes v6 if v7 was hiding it. Firms that already applied a withdrawn version are flagged to the content team.
- **Idempotent recompute.** On each event the consumer reads the current version graph and recomputes the affected rows instead of applying a delta. Duplicates, delays and reordering converge. Decisions are append-only.
- **Honest indicator.** Each engagement file shows one of three states: `UP_TO_DATE`, `UPDATES_PENDING (n)` or `UNKNOWN` (not yet indexed or not verified). The system never shows "up to date" for a file it has not verified.

## 4. Human-readable summaries

**Generation.** The input is the deterministic JSON diff between two versions, never engagement content. A summary is computed once per version pair and shared by all firms, because templates are not firm-specific.

1. *Deterministic base:* group the diff into change items (path, change type, old and new values) and render a rule-based plain-language text. It is available immediately after publication, so the indicator never waits for the model.
2. *LLM rewrite:* a pinned model snapshot (Amazon Bedrock), temperature 0 and a versioned prompt rewrite the change items for non-technical readers. The output is structured, and every statement cites at least one change item.
3. *Validation:* every change item is covered, every statement is grounded in a cited item, and every number, name and identifier in the text appears in the diff. On failure the system retries once and then falls back to the deterministic text, flagged as such. An unvalidated narrative is never shown.

**Evaluation.**

- *Offline:* a golden set of real diffs curated by the content team. Any prompt or model change must pass regression on coverage, unsupported-claim rate and readability before release.
- *Online:* validation pass rate, fallback rate, user "unclear" feedback and sampled human review by the content team, focused on high-impact changes (calculations, regulatory references).

**Defensibility (a summary shown in March may be defended in November).**

- Each summary is an immutable artifact (S3 with Object Lock), identified by `hash(fromVersion, toVersion, diffHash, promptVersion, modelId)` and stored with the diff hash, prompt version, model id, validation report and timestamp.
- The decision record stores the summary id and hash that the user saw, and a snapshot of the text is kept in the regional audit store. Regenerating a summary creates a new version; it never replaces one already shown.
- The raw diff is always one click away as ground truth. Retention follows the firm's regulatory retention period (7 years assumed, to be confirmed).

## 5. Scale, cost and operations

| Quantity | Calculation | Result |
| -- | -- | -- |
| Publishes | 40 products × 1 per week | ~40/week, ~6/day, ~170/month |
| Files per product (average) | 800,000 ÷ 40 | 20,000 (plan for 5× skew: 100,000) |
| Detecting by loading (rejected) | 20,000 × 1 min | 333 h per publish; still 3.3 h at 100 concurrent loads |
| Detecting by index (chosen) | 20,000 rows ÷ 1,000 per batch × ~50 ms | ~1 s (~5 s at 100,000); to be confirmed by load test |
| Largest firm | 40,000 files | ~2 s of index updates vs. 667 h of loads |
| Backfill | 800,000 × 1 min | 13,333 loader-hours: 5.6 days at 100 concurrent, 2.8 days at 200 |
| Reconciliation | 1% sample per week = 8,000 loads | 133 loader-hours, ~0.8 concurrent loads over the week |
| Index size | 800,000 × ~1 KB + ~1.6 M pending rows × ~0.2 KB | ~1.1 GB, a small managed relational instance per region |
| Summaries | ~170 publishes × ≤ 5 distinct baselines × ≤ $0.25 | ~850 summaries, ≤ ~$210/month |

**Cost.** Inference scales with the number of distinct version pairs (hundreds per month), not with the 800,000 engagement files, and there are no loads on the publish path. The recurring cost is dominated by fixed infrastructure (three regional cells and Kafka), sized small. I cannot show the total against *$X* without the budget. The one-time backfill cost falls mostly on the team that owns the load operation, so its cap and schedule must be agreed with them.

**Fan-out worker (Part 2).** On `TemplateVersionPublished`, the worker recomputes the affected rows from the index. For files in `UNKNOWN` state it enqueues a low-priority load task, which is the same path the backfill uses.

- *Concurrency:* a global token bucket plus adaptive concurrency (AIMD on timeouts and 429s), a circuit breaker, and a per-firm concurrency cap so the 40,000-file firm cannot starve the median 40-file firm.
- *Partitioning:* by engagement file id, not firm id, to avoid hot partitions.
- *Idempotency:* the key is `(engagementFileId, operation, targetVersion, lineageRevision)`. Retries use backoff with jitter, and exhausted items go to a dead-letter queue.

**Observability and SLOs.**

| SLI | SLO |
| -- | -- |
| Publish → indicator visible (p50 / p99) | < 5 s / < 60 s |
| Publish → validated summary (p95) | < 5 min (the deterministic text is shown immediately) |
| Index accuracy (sampled mismatch rate) | < 0.1% |
| Index coverage before enabling a firm | 100% of its active files indexed |
| Consumer lag / dead-letter depth | < 30 s / 0 after 24 h |
| Pending-update API availability | 99.9% |

Dashboards per region, traces keyed by `eventId`, alerts on lag, dead-letter depth, downstream saturation and backfill ETA. Logs never contain engagement content.

## 6. Production evolution and failure recovery

**Rollout without a maintenance window** (every step is additive and reversible):

1. *Product:* add lineage (parents, status) and the outbox as nullable columns. Parents for historical versions are backfilled (linear history assumed unless the content team provides the branches). The existing publish path is unchanged.
2. *Engagement:* add asynchronous hooks (create, load, apply, decline). A hook failure never fails a user operation. The Update Index runs in shadow mode, invisible to users.
3. *Backfill:* existing files are indexed by the fan-out worker, rate-limited, resumable (per-firm cursor) and idempotent. Any file a user opens is indexed immediately, so the most-used files come first.
4. *Validate:* reconcile a sample of loads against the index until the accuracy SLO is met.
5. *Enable:* feature flag per firm (internal → pilot firms → region by region) with a kill switch. Rollback is turning the flag off; no schema contraction is needed.

**Failure recovery:**

| Failure | Handling |
| -- | -- |
| Event lost, duplicated or reordered | Outbox, idempotent upsert and recompute from the version graph. An hourly anti-entropy check compares each product's `lineageRevision` with the region's watermark and triggers a recompute on mismatch. |
| Consumer down or slow | Replay from Kafka retention; lag alert. |
| Poison message | Retry with backoff, then dead-letter queue and alert; replay after the fix. |
| Downstream load slow or failing | Token bucket, circuit breaker and durable queued work; items are resumable. |
| Index drift or corruption | Self-heal on load, sampled reconciliation, or rebuild per firm (the index is derived data). |
| Summary generation or validation fails | Deterministic fallback; retry later; the indicator is never blocked. |
| Region outage | Cells are independent, so the blast radius is one region. Product and summaries remain readable. |

## 7. Data residency

- **Regional cells** (EU, Canada, default) contain the Engagement Engine, Engagement Database and Update Index, Storage (in-region bucket and KMS keys), Cache, consumers and the audit store. Engagement content, decisions and logs never leave the region.
- **Global:** the Product Domain, the summaries and the Kafka events contain no firm data and are replicated to the regions (MSK Replicator or equivalent).
- **LLM:** the model only sees template diffs, so no client-confidential data reaches the inference provider and one global summarization is compliant.
- **Routing:** the User Domain maps each firm to its region and the edge routes requests to that cell. The only cross-region data is template identifiers and versions.

## 8. Trade-offs, risks and scope

**Trade-offs.**

- *Update Index vs. loading for the truth:* the index is eventually consistent. Bounded staleness (SLO) and a reconciliation cost are accepted in exchange for seconds instead of hundreds of hours.
- *Materialized pending rows vs. computing on read:* materialized, for fast counts and filters.
- *One global summary vs. one per firm:* global is cheaper, consistent and auditable. It cannot be personalized per engagement, which is acceptable because applying content is out of scope.
- *Kafka vs. SNS/SQS:* Kafka gives per-key ordering, replay and one consumer group per region, but ~40 events per week is far below what it is built for. SNS plus SQS would be enough, so this should be revisited if the operating cost outweighs the benefit.

**Requirement I would challenge: "real time, within seconds of publication".** Publishes are weekly per product and firms decide over days, so seconds versus one minute has no business value. A strict seconds target would also push unvalidated model output to users, which conflicts with defensibility. The design meets seconds for the indicator in the typical case, and I propose defining "real time" as p99 < 60 s for the indicator and p95 < 5 min for the validated summary.

**Riskiest part: the correctness of baselines and lineage.** A false "up to date" is silent, and the source data is the least known today (parents of historical versions, baselines of existing files). Mitigations: the `UNKNOWN` state, self-healing on load, sampled reconciliation with an accuracy SLO, and per-firm enablement only at 100% coverage. The second risk is a summary that omits or invents a change, which is mitigated by validation and the deterministic fallback.

**Deliberately left out:** applying template content; notifications (email, push); remediation for firms that applied a later-withdrawn version; moving a firm between regions; per-engagement impact analysis (it would require loading engagement content); localization of summaries; disaster-recovery targets (RPO/RTO); UI design; identity-provider details.

## 9. Assumptions

- The vendor chosen for this design is AWS.
- The "volatile financial" fields are provided by an external, third-party component named "Financial System".
- There is an exclusive component to store the engagement zip files (Storage System), and a separate Cache component that spins up frequently accessed, immutable artifacts.
- A load is ~1 minute per engagement file (hard constraint), and the team that owns it can grant a bounded concurrency (assumed ≥ 100) for the backfill.
- The diff tool is deterministic for a given version pair.
- Engagement file identifiers contain no client content, so they can be used in cross-region metadata.
- The content team can provide or confirm the parents and market of historical template versions.
- The budget *$X* is not known; the cost analysis shows what scales and what does not.

## Appendix A. API overview

### Product Engine

| API | Access | Description |
| -- | -- | -- |
| `GET /products` | private | List all the products the user has access to |
| `GET /products/{productId}/templates` | private | List all the templates of a product that the user has access to |
| `GET /products/{productId}/templates/{templateId}` | private | Get a specific template by product id and template id |
| `GET /products/{productId}/templates/{templateId}/versions` | private | List the versions of a template, with status and parents |
| `GET /products/{productId}/templates/{templateId}/versions/{version}` | private | Get a specific template version |
| `GET /products/{productId}/templates/{templateId}/versions/compare?from={origin}&to={target}` | private | Generate the diff and the summary by comparing the origin and target versions |
| `POST /products/{productId}/templates/{templateId}/versions` | internal | Create a draft version (content team) |
| `POST /products/{productId}/templates/{templateId}/versions/{version}/publish` | internal | Publish a version and write the outbox event |
| `POST /products/{productId}/templates/{templateId}/versions/{version}/withdraw` | internal | Withdraw a published version |

### Engagement Engine

| API | Access | Description |
| -- | -- | -- |
| `GET /engagement-files` | private | List the caller's engagement files with their update status (filter by `updateStatus`, `productId`; paginated) |
| `POST /engagement-files` | private | Create a new engagement file based on the latest available template version |
| `GET /engagement-files/{id}` | private | Load an engagement file by id (slow, ~1 minute) |
| `GET /engagement-files/{id}/updates` | private | List the pending updates of a file with their summaries (served from the index, no load) |
| `POST /engagement-files/{id}/updates/{targetVersion}/apply` | private | Apply an update to the file based on the target template version |
| `POST /engagement-files/{id}/updates/{targetVersion}/decline` | private | Decline an update to the file based on the target template version |
| `GET /updates` | private | List all the pending updates applicable to the firm's engagement files, grouped by target version |

### User Management

| API | Access | Description |
| -- | -- | -- |
| `POST /login` | public | Allow the sign-on to the system and issue a token (the only public API) |
| `GET /me/permissions` | private | Permissions and entitlements of the caller |
| `GET /firms/{firmId}/residency` | internal | Region assigned to a firm |
