// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operators::window::tumbling::{Params, drive_reclaiming};

#[test]
fn a_generated_corpus_stays_foldable_while_the_sweep_runs_underneath_it() {
	// A generated corpus interleaves inserts, removes, updates, seals and sweeps in an order nobody
	// chose, reaching states a hand-written case does not think to build. The bounds relax once
	// reclamation is on: a stranded row is permitted, a stream that stops folding is not.
	let outcome = drive_reclaiming(
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
		20,
		true,
	);

	// Without this the test passes just as well against a sweep that never reached a single group.
	assert!(
		!outcome.reclaimed.reclaimed_nothing(),
		"the sweep must actually have reclaimed something, or every assertion above is vacuous: {:?}",
		outcome.reclaimed
	);
}
