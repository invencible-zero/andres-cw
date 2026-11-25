package com.invencible.labs.cw.fanout.model;

import java.time.Instant;

/** What a load reveals: the baseline the engagement is really on. Never any engagement content. */
public record LoadResult(long fileId, String baselineVersion, Instant loadedAt) {
}
