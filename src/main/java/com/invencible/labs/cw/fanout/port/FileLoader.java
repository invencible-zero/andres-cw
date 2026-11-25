package com.invencible.labs.cw.fanout.port;

import com.invencible.labs.cw.fanout.model.LoadRequest;
import com.invencible.labs.cw.fanout.model.LoadResult;

/**
 * The downstream call owned by another team: it rehydrates an engagement file (about one minute) and reveals its real
 * baseline. It has limited capacity, and protecting it is the main job of the worker.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>It blocks. The worker calls it on a virtual thread, so blocking is cheap.</li>
 *   <li>It <b>must enforce {@link LoadRequest#deadline()}</b> and throw {@code LoadException(TIMEOUT)} when it
 *       expires. The worker holds a concurrency permit for the whole call and relies on this to get it back.</li>
 *   <li>It is safe to call repeatedly with the same idempotency key. A load only reads the engagement, so a duplicate
 *       costs capacity, not correctness.</li>
 *   <li>It reports failures only as {@code LoadException}, and it honours interruption (shutdown).</li>
 * </ul>
 */
public interface FileLoader {

	LoadResult load(LoadRequest request) throws InterruptedException;
}
