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

const SEED: u64 = 2_112_662_219_028_287_888;

#[test]
fn a_removal_above_the_reclaim_cutoff_retracts_its_windows_rather_than_stranding_them() {
	// A coordinate above the reclaim cutoff was never reclaimable, so its removal has to take effect
	// in full. The contract permits a reclaimed window to strand a stale value; a sweep that erases
	// state addressing windows outside its own cutoff is reaching past the frontier it reported.
	drive_reclaiming(SEED, params(), 20, true);
}

#[test]
fn the_same_corpus_with_the_sweep_switched_off_diverges_nowhere() {
	// Decides how to read the failure above: same seed, same params, same grid and sink ttl, only
	// the sweep is off. If this diverges too then the corpus or the operator is at fault and
	// reclamation is incidental; if only the reclaiming twin diverges, the sweep is the cause.
	let outcome = drive_reclaiming(SEED, params(), 0, true);

	assert!(
		outcome.reclaimed.reclaimed_nothing(),
		"precondition: reclaim_pct of zero must sweep nothing, or this is not a control: {:?}",
		outcome.reclaimed
	);
}
