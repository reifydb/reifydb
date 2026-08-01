// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operators::window::sliding::{Params, drive_reclaiming};

fn params() -> Params {
	Params {
		size_secs: 30,
		slide_secs: 10,
		grace_secs: 45,
		groups: 3,
		steps: 60,
		max_batch: 4,
		coord_span_ms: 400_000,
		remove_pct: 25,
		update_pct: 15,
		seal_pct: 30,
	}
}

const SEED: u64 = 7_679_903_394_466_761_495;

#[test]
fn the_same_corpus_with_the_sweep_switched_off_diverges_nowhere() {
	// The control that decides how to read a failure below: same seed, params, grid and sink ttl with
	// only the sweep off. If this diverges too, the corpus or the operator is at fault and
	// reclamation is incidental.
	let outcome = drive_reclaiming(SEED, params(), 0, true);

	assert!(
		outcome.reclaimed.reclaimed_nothing(),
		"precondition: reclaim_pct of zero must sweep nothing, or this is not a control: {:?}",
		outcome.reclaimed
	);
}

#[test]
fn the_data_phase_alone_leaves_the_same_corpus_consistent() {
	// The second control, which localises a failure to a phase: same corpus and sweeps but no sink row
	// ttl, so the identity phase is skipped and only accumulators are erased. Passing here while the
	// two-phase twin fails puts the difference in the identity phase.
	let outcome = drive_reclaiming(SEED, params(), 20, false);

	assert!(
		!outcome.reclaimed.reclaimed_nothing(),
		"precondition: the data phase must still reach something: {:?}",
		outcome.reclaimed
	);
}

#[test]
fn a_generated_corpus_of_overlapping_windows_survives_the_sweep_running_underneath_it() {
	// Sliding is where the sweep meets the most state at once, and where a duplicate is hardest to see
	// by value alone: with many windows per group a stray row can coincide with some total. The key
	// catches it - the view is rekeyed on (group, window start) before comparing.
	let outcome = drive_reclaiming(SEED, params(), 20, true);

	// Without this the test passes just as well against a sweep that never reached a single group.
	assert!(
		!outcome.reclaimed.reclaimed_nothing(),
		"the sweep must actually have reclaimed something, or every assertion above is vacuous: {:?}",
		outcome.reclaimed
	);
}
