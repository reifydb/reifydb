// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The join is the only operator that declares keyspace spans and a node-scope mapping span, so it
//! is the only route to sweep phases three and four. Everything below drives those two phases.

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
		// Deliberately none, and it is what makes the mapping phase unreachable here. This harness's
		// view never expires a row, so it models a sink with no retention whatever is declared; a sink
		// ttl would let the sweep drop a mapping while the modelled row stays published forever, and
		// the duplicate that follows would be the fixture's doing rather than the operator's.
		sink_row_ttl: None,
		static_right: 0,
	}
}

/// Refuses a run that proves nothing from either end.
///
/// Both directions matter and they fail for opposite reasons. A sweep that reached no published row
/// leaves every claim below holding because reclamation never happened; a claim that gave up on the
/// whole view holds because it stopped asserting. The interesting runs are the ones in between, and
/// only a two-sided check keeps the suite inside them.
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
	// The control that decides what a failure above means. Identical seed, identical parameters,
	// identical ttls - only the sweep share is zero. Red here would mean the corpus found an ordinary
	// join defect and reclamation is a bystander; green here is what pins a failure above to the
	// sweep.
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
	// The control that makes the two above mean something. Same corpus, same sweep share, no ttl:
	// `retention_scale` is none, the node grids undeclared, and the reclaim driver skips it in
	// silence. If this reported reclamation the suite would be sweeping on a horizon nobody
	// declared, and the two runs above would be measuring that instead of the ttls they set.
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
