// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operators::window::tumbling::{Params, drive_reclaiming};

#[test]
fn a_generated_corpus_stays_foldable_while_the_sweep_runs_underneath_it() {
	// The gap this closes: every reclaim assertion in the tree drove one or two hand-placed rows.
	// A generated corpus interleaves inserts, removes, updates, seals and now sweeps in an order
	// nobody chose, which is where the sweep meets states a hand-written case does not think to
	// build - a group swept between a seal and the update that follows it, a row removed after the
	// state that would retract it is gone.
	//
	// The bounds relax on purpose once reclamation is on: a stranded row is permitted, because the
	// state that would have retracted it was erased and the operator publishes nothing on a sweep.
	// What stays strict is that the stream remains foldable and that no row appears which was never
	// admitted.
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

	// Without this the test passes just as well against a sweep that never reached a single group,
	// which is the state the whole tree was in before: green, and evidence of nothing.
	assert!(
		!outcome.reclaimed.reclaimed_nothing(),
		"the sweep must actually have reclaimed something, or every assertion above is vacuous: {:?}",
		outcome.reclaimed
	);
}
