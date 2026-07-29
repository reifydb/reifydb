// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operators::window::rolling::{Params, drive};

#[test]
fn a_late_row_does_not_withdraw_the_group_it_belongs_to() {
	// apply_rolling drops a late row's bucket before the engine runs, but used to leave the group
	// in `touched`. finish_rolling_results then read "in touched, produced no result" as "this
	// group is now empty" and emitted a Diff::remove for a live aggregate nothing had asked it to
	// touch. A single stray old timestamp could therefore delete a healthy group's total.
	//
	// This guards the COMBINATION, and has to: the orphaned group is only observable while the
	// withdrawal fallback exists to act on it. Reverting `touched.retain` alone changes no output
	// at all, so no corpus can pin it by itself. Verified by mutation - this seed fails with both
	// `touched.retain` removed AND the fallback restored, and passes with only the fallback
	// restored, which is what makes the orphan the demonstrated cause rather than a coincidence.
	//
	// The seed was re-derived on 2026-07-29. The original recording
	// (16057352923150615165) silently stopped reproducing when the driver gained its update
	// branch: a new draw per step shifted the whole RNG stream, so the seed still ran, still
	// passed, and covered something else entirely.
	let corpus = drive(
		1,
		Params {
			size_secs: 60,
			grace_secs: 0,
			groups: 4,
			steps: 40,
			max_batch: 5,
			coord_span_ms: 600_000,
			remove_pct: 30,
			update_pct: 20,
			seal_pct: 20,
		},
	);
	corpus.assert_pinned(0x8bf5_36e2_265d_18ee);
}

#[test]
fn retracting_an_already_evicted_contribution_does_not_withdraw_the_group() {
	// The same "no result means gone" confusion reached through the other door, found by the
	// grace-heavy sweep. With grace wider than the interval a coordinate can be new enough to
	// admit (>= ledger - (size + grace)) while already older than the trailing window
	// (<= ledger - size). Retracting one is a genuine no-op, the engine returns no result, and
	// finish_rolling_results used to withdraw the group on exactly that silence.
	//
	// This seed drives exactly one such retraction: group 3, coordinate 76413, against an
	// eviction cutoff of 81775 and an admission horizon of 36775. So 76413 is comfortably inside
	// the admission window and already outside the trailing one, and group 3 still holds retained
	// contributions at that point - the precise shape that used to withdraw a live group.
	//
	// The deterministic hand-built counterpart is window_rolling.rs's
	// retracting_a_row_that_has_already_left_the_window_leaves_the_group_intact.
	let corpus = drive(
		11,
		Params {
			size_secs: 30,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
	corpus.assert_pinned(0x5aed_fddf_9fb6_e799);
}
