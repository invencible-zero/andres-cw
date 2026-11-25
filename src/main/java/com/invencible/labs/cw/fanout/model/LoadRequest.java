package com.invencible.labs.cw.fanout.model;

import java.time.Duration;

/**
 * What the downstream receives.
 *
 * @param idempotencyKey a stable key for this unit of work. A timeout is ambiguous (the load may have completed), so a
 *                       retry carries the same key and the downstream can de-duplicate if it wants to.
 * @param deadline       how long the caller is willing to wait. The downstream client must enforce it.
 */
public record LoadRequest(long fileId, String idempotencyKey, Duration deadline) {
}
