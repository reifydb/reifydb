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
	drive(
		12_758_060_916_095_492_152,
		Params {
			size_secs: 30,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			seal_pct: 30,
		},
	);
}
