// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The join is the only operator that declares keyspace spans, so it is the only route to the keyspace
//! sweep phase. The mapping phase is deliberately out of reach here - see `sink_row_ttl` below.

use reifydb_value::value::duration::Duration;

use crate::operators::join::{JoinReclaim, Params, Variant, drive_reclaiming};

fn ttl(secs: i64) -> Option<Duration> {
	Some(Duration::from_seconds(secs).expect("a ttl in seconds is representable"))
}

pub fn params(variant: Variant) -> Params {
	Params {
		variant,
		keys: 3,
		right_pct: 50,
		none_pct: 10,
		rekey_pct: 30,
		steps: 60,
		max_batch: 4,
		max_live: 40,
		remove_pct: 20,
		update_pct: 25,
		coord_span_ms: 400_000,
		left_ttl: ttl(30),
		right_ttl: ttl(30),
		tick_pct: 20,
		// Deliberately none, which leaves both the identity and mapping phases unreachable: the mapping
		// cutoff is clamped to the identity cutoff, and that is derived from this ttl. This harness's
		// view never expires a row, so a sink ttl would strand mappings by the fixture's doing.
		sink_row_ttl: None,
		static_right: 0,
	}
}

/// Refuses a run that proves nothing from either end: a sweep that reached no published row leaves
/// every claim holding for want of reclamation, and a claim that gave up on the whole view holds
/// because it stopped asserting.
#[track_caller]
pub fn assert_bites(run: &JoinReclaim, label: &str) {
	assert!(
		!run.outcome.reclaimed.reclaimed_nothing(),
		"{label}: the sweep retired nothing, so every bound this corpus checked held for want of \
		 reclamation rather than in spite of it: {:?}",
		run.outcome.reclaimed
	);
	assert!(
		run.envelope.reached > 0,
		"{label}: the sweep ran but never reached a row the view had published, so the legality \
		 envelope was never exercised: {:?}",
		run.outcome.reclaimed
	);
	assert!(
		run.envelope.pinned > 0,
		"{label}: the claim ends up constraining nothing at all ({:?}), which is a suite that passes \
		 by saying nothing",
		run.envelope
	);
}

#[test]
fn an_inner_join_stays_foldable_while_both_its_sides_age() {
	// The keyspace phase retires one side's group while the other side's rows for the same key stay
	// live, which is the state no hand-written fixture builds: the operator holds half a key.
	let run = drive_reclaiming(9_182_733_645_512_009_117, params(Variant::inner()), 20);
	run.assert_clean();
	assert_bites(&run, "inner");
}

#[test]
fn a_left_join_stays_foldable_while_both_its_sides_age() {
	// A left join publishes unmatched rows, so reclaiming the right side of a key can turn a joined
	// row back into an unmatched one - a transition an inner join cannot make and the only shape
	// where the sweep changes a row's contents rather than only stranding it.
	let run = drive_reclaiming(4_402_118_907_336_215_884, params(Variant::left()), 20);
	run.assert_clean();
	assert_bites(&run, "left");
}

#[test]
fn the_same_corpus_with_the_sweep_switched_off_diverges_nowhere() {
	// The control that decides what a failure above means: identical seed, parameters and ttls with
	// only the sweep share zero. Red here would mean an ordinary join defect with reclamation a
	// bystander.
	let run = drive_reclaiming(9_182_733_645_512_009_117, params(Variant::inner()), 0);
	run.assert_clean();
	assert!(
		run.outcome.reclaimed.reclaimed_nothing(),
		"the control must not sweep at all, but it reported {:?}",
		run.outcome.reclaimed
	);
}

#[test]
fn a_join_with_no_ttl_on_either_side_is_never_swept() {
	// The control that makes the two above mean something: same corpus and sweep share, no ttl, so the
	// node grids undeclared and the driver skips it. Reclamation here would mean the suite sweeps on a
	// horizon nobody declared.
	let run = drive_reclaiming(
		9_182_733_645_512_009_117,
		Params {
			left_ttl: None,
			right_ttl: None,
			..params(Variant::inner())
		},
		20,
	);
	run.assert_clean();
	assert!(
		run.outcome.reclaimed.reclaimed_nothing(),
		"a join that declares no ttl retains forever, but the sweep reported {:?}",
		run.outcome.reclaimed
	);
	assert_eq!(run.envelope.reached, 0, "nothing was reclaimed, so nothing may have left the claim's reach");
}

#[test]
fn only_the_side_that_declares_a_ttl_ages() {
	// Each side ages on its own keyspace at its own ttl. A sweep that read one ttl and applied it to
	// both would retire right-side state a join with an infinite right ttl must keep, and the
	// symptom is a left row that stops matching - which reads as an ordinary join bug.
	let run = drive_reclaiming(
		1_557_320_998_441_006_733,
		Params {
			right_ttl: None,
			..params(Variant::inner())
		},
		20,
	);
	run.assert_clean();
	assert_bites(&run, "left side only");
}
