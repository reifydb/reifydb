// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operators::window::tumbling::{Params, drive};

#[test]
fn a_window_expired_by_the_sweep_is_not_resurrected_by_a_later_event() {
	// Recorded from a chaos failure: make test-chaos SEED=12758060916095492152
	// FILTER=window_tumbling_grace_chaos_20
	//
	// Window [270000, 300000) with size 30s + grace 45s reached a high water of 288553. A seal at
	// ledger 356496 expired it, because the sweep closes a window at window_start + size + grace
	// (345001 <= 356496). The admission gate disagreed: it closed at last_event + size + grace
	// (363554 > 356496), so it kept admitting events into a window whose state the sweep had
	// already reclaimed. The window reappeared holding only the newest event's value instead of
	// the sum of every event in it.
	let corpus = drive(
		12_758_060_916_095_492_152,
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
	corpus.assert_pinned(0x5431_5649_ed9e_0862);
}

#[test]
fn an_event_at_coordinate_zero_is_still_refused_by_a_closed_window() {
	// Found by window_tumbling_random_chaos, which draws its configuration from the seed; no
	// hand-picked config had a window size small enough relative to the corpus for coordinate 0
	// to still be generated after the ledger had run well past it.
	//
	// Step 6 inserts (g=3, coord=0, v=66). Window [0, 1000) closed long before: with size 1s and
	// grace 3s it seals at 0 + 4000 + 1, and the ledger was already 13212. The oracle refuses the
	// row. The operator admitted it and published a g=3 window holding 66.
	//
	// gate_and_arm_seals derives the bucket's event time as max(prior_last, batch_last) and then
	// `continue`s when that is 0, before the seal test runs. A coordinate of 0 makes batch_last 0,
	// and a window with no meta makes prior_last 0, so the bucket skips the gate entirely rather
	// than being refused. 0 is being used as a "no event time" sentinel in a space where 0 is a
	// legitimate coordinate - the same confusion as the seal_due_windows threshold collision.
	let corpus = drive(
		1_289_918_683_737_022_840,
		Params {
			size_secs: 1,
			grace_secs: 3,
			groups: 3,
			steps: 28,
			max_batch: 3,
			coord_span_ms: 17_000,
			remove_pct: 23,
			update_pct: 4,
			seal_pct: 24,
		},
	);
	corpus.assert_pinned(0x008b_3939_0cef_a2c0);
}
