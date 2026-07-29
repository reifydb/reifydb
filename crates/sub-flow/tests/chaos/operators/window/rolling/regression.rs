// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operators::window::rolling::{Params, drive};

#[test]
fn a_late_row_does_not_withdraw_the_group_it_belongs_to() {
	// Recorded from a chaos failure: make test-chaos SEED=16057352923150615165
	// FILTER=window_rolling_sum_chaos_0
	//
	// Step 17 inserted into all four groups. Groups 2 and 4 came out with correct totals; groups
	// 1 and 3 vanished from the view entirely. What distinguished them is that groups 1 and 3
	// were the ones whose only row in that batch was too old to admit.
	//
	// apply_rolling drops a late row's bucket before the engine runs, but left the group in
	// `touched`. finish_rolling_results then read "in touched, produced no result" as "this group
	// is now empty" and emitted a Diff::remove for a live aggregate nothing had asked it to
	// touch. Instrumentation on this seed showed it exactly: dropped=1 touched=3 admitted=2, the
	// one orphaned hash being precisely the group that disappeared.
	drive(
		16_057_352_923_150_615_165,
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
	drive(
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
}
