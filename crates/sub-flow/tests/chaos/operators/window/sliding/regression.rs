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
	// The corpus reaches this at step 20, and the whole point of pinning it is that the shape is
	// small enough to read:
	//
	//   step  4  insert row 1, group 2, coord 371517, value 76
	//   step 13  remove row 4, same group, coord 30552 - an OLD row, so an old batch position
	//   step 16  the sweep retires that group's DATA at cutoff 296516
	//   step 20  remove row 1
	//
	// Coordinate 371517 sits ABOVE the cutoff, so nothing about it was reclaimable and its removal
	// has to take effect in full. It used to leave the three windows covering it published holding
	// 76: step 13 dragged the partition's activity stamp backwards to bucket 6, so step 16 retired a
	// partition that still held row 1, taking with it the row-index that addresses it. The remove at
	// step 20 then found no windows and retracted nothing.
	//
	// The contract permits a RECLAIMED window to strand a stale value, which is why the model still
	// permits the group's 41s - that coordinate is 30552, below the cutoff. It permits nothing for a
	// coordinate the sweep could never have reached. A sweep that erases state addressing windows
	// outside its own cutoff is reaching past the frontier it reported.
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
