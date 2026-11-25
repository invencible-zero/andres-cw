package com.invencible.labs.cw.fanout.model;

/** Whether the index can trust an engagement file's baseline (ADD section 3, "honest indicator"). */
public enum VerificationState {
	/** The baseline was confirmed by a load or by a hook, so pending updates can be derived from it. */
	VERIFIED,
	/** Not yet verified. The file must never be shown as "up to date". */
	UNKNOWN
}
