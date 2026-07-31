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
	// The control, and the thing that decides how to read a failure in the test below. Same seed,
	// same params, same grid and sink ttl - only the sweep is off. If this diverges, the corpus or
	// the operator is at fault and reclamation is incidental. If only the reclaiming twin diverges,
	// the difference is the sweep.
	let outcome = drive_reclaiming(SEED, params(), 0, true);

	assert!(
		outcome.reclaimed.reclaimed_nothing(),
		"precondition: reclaim_pct of zero must sweep nothing, or this is not a control: {:?}",
		outcome.reclaimed
	);
}

#[test]
fn the_data_phase_alone_leaves_the_same_corpus_consistent() {
	// The second control, and the one that localises a failure in the test below to a phase. Same
	// corpus, same sweeps, but no sink row ttl - so `identity_span` is none and `reclaim_nodes` skips
	// phase two entirely. Accumulators are still erased; only the mappings that address the rows
	// naming them survive.
	//
	// If this passes while the two-phase twin fails, the difference is the identity phase: a mapping
	// retired while the row it names is still published.
	let outcome = drive_reclaiming(SEED, params(), 20, false);

	assert!(
		!outcome.reclaimed.reclaimed_nothing(),
		"precondition: the data phase must still reach something: {:?}",
		outcome.reclaimed
	);
}

#[test]
fn a_generated_corpus_of_overlapping_windows_survives_the_sweep_running_underneath_it() {
	// Sliding is where the sweep meets the most state at once: overlapping windows put every
	// coordinate in several of them, so one group carries many live rows and a sweep reaching any of
	// them has more ways to go wrong than in the tumbling case.
	//
	// It is also where a duplicate is hardest to see by value alone. The oracle permits one total per
	// window per group, so with many windows per group a stray second row has many totals it can
	// coincide with and be absorbed by the multiset bound. What catches it here is the key - the
	// oracle rekeys the view on (group, window start) before comparing, so two rows for one window
	// are reported however their values land.
	let outcome = drive_reclaiming(SEED, params(), 20, true);

	// Without this the test passes just as well against a sweep that never reached a single group.
	assert!(
		!outcome.reclaimed.reclaimed_nothing(),
		"the sweep must actually have reclaimed something, or every assertion above is vacuous: {:?}",
		outcome.reclaimed
	);
}
