package com.invencible.labs.cw.fanout.port;

import com.invencible.labs.cw.fanout.model.VersionGraph;

/** Reads template versions through the Product Engine API, never its database (ADD section 2). */
public interface VersionGraphSource {

	/** The current graph of a template, with the lineage revision it was read at. */
	VersionGraph load(long templateId);
}
