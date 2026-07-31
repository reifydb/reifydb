// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The foldable-forever contract, driven against a real sweep.
//!
//! Reclamation deletes operator state and publishes no diffs, so rows it stranded stay in the view
//! while the state that would have retracted them is gone. The contract that follows from that is
//! not "the view stays correct" - no operator can promise it, and several structurally cannot - but
//! "whatever the sweep deletes, the operator never afterwards publishes a diff a sink cannot apply".
//! Stale rows are permitted; a remove of a row that was never published is not.
//!
//! Two things make this assertable at all, and both are recent: the harness can now age a group (it
//! registers an activity grid and stamps coordinates from the rows), and `MaterializedView` now
//! reports an update that lands on a live row instead of silently overwriting it.

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_sub_flow::operator::window::operator::WindowOperator;
use reifydb_testing_chaos::operator::{reclaim::Reclaimed, session::Session};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

use crate::{
	framework::{generator, harness::Harness},
	operators::window::{WindowSpec, build},
};

// Every shape below is sized at this, and with zero grace it is the horizon too.
// The suite does not get to pick that number: `resolve_horizon` lets an operator's own seal span
// override whatever a harness declares, so a hand-chosen span would describe a node the engine
// cannot register and every cutoff derived from it would be arithmetic that never ships.
const WINDOW_SECS: i64 = 60;

const SPAN_MS: u64 = WINDOW_SECS as u64 * 1_000;

// Sixteen buckets per horizon, so a 60s span grids at 3.75s.
const GRID_WIDTH_MS: u64 = SPAN_MS / 16;

// Arrivals sit one second past the epoch, not on it. Rolling's eviction range is inclusive of its
// cutoff, and that cutoff saturates to zero while the seal ledger is still at the epoch, so a row
// coordinated at exactly zero is evicted the moment it lands. That left rolling holding no data at
// all, which silently voided every per-shape assertion below rather than failing one.
const ARRIVAL_MS: u64 = 1_000;

// A group is due once its activity bucket falls strictly below the cutoff's. The data phase cuts at
// watermark - span, and every arrival in this suite lands in bucket zero, so the watermark has to
// reach one full grid width past the span before anything is reclaimable.
const SWEEP_MS: u64 = SPAN_MS + GRID_WIDTH_MS;

// One millisecond short of due, for the no-op control.
const EARLY_SWEEP_MS: u64 = SWEEP_MS - 1;

// The identity phase cuts at watermark - ttl - slack, where the data phase cuts at watermark - span.
// With the sink ttl set equal to the span, identity therefore trails data by exactly one grid width,
// and a group in bucket zero is reachable by the data phase a full width before its mapping is. That
// gap is the whole point of the split: a mapping retired while the row naming it is still published
// makes the next event on that key mint a second row beside it.
const IDENTITY_SWEEP_MS: u64 = SPAN_MS + 2 * GRID_WIDTH_MS;

fn spec() -> WindowSpec {
	WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::default(),
	}
}

fn harness() -> Harness<WindowOperator> {
	// The sink ttl is what bounds the identity phase. Without it `identity_span` is none and
	// `reclaim_nodes` skips phase two outright, so every "the identity half survived" assertion in
	// this file would hold because the phase never ran rather than because the split works.
	let span = Duration::from_seconds(WINDOW_SECS).expect("span is representable");
	Harness::new(|runtime| build(&spec(), runtime)).with_activity_grid().with_sink_row_ttl(span)
}

fn at(ms: u64) -> DateTime {
	DateTime::from_timestamp_millis(ms).unwrap()
}

// The four window shapes, all driven through the same corpus. They are separate engines with
// separate emit paths, and the Insert-versus-Update decision is made independently in each, so a fix
// in one says nothing about the others. Session is included because it runs the tumbling engine on a
// different anchor, which is exactly the kind of difference that hides a shared defect.
fn kinds() -> Vec<(&'static str, WindowKind)> {
	let secs = |n: i64| WindowSize::Duration(Duration::from_seconds(n).unwrap());
	vec![
		(
			"tumbling",
			WindowKind::Tumbling {
				size: secs(60),
			},
		),
		(
			"sliding",
			WindowKind::Sliding {
				size: secs(60),
				slide: secs(30),
			},
		),
		(
			"rolling",
			WindowKind::Rolling {
				size: secs(60),
				lag: None,
			},
		),
		(
			"session",
			WindowKind::Session {
				gap: Duration::from_seconds(60).unwrap(),
			},
		),
	]
}

fn harness_of(kind: WindowKind) -> Harness<WindowOperator> {
	// The sink ttl is what bounds the identity phase. Without it `identity_span` is none and
	// `reclaim_nodes` skips phase two outright, so every "the identity half survived" assertion in
	// this file would hold because the phase never ran rather than because the split works.
	let span = Duration::from_seconds(WINDOW_SECS).expect("span is representable");
	let spec = WindowSpec {
		kind,
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::default(),
	};
	Harness::new(|runtime| build(&spec, runtime)).with_activity_grid().with_sink_row_ttl(span)
}

#[test]
fn no_window_shape_double_publishes_a_key_that_wakes_after_reclamation() {
	// The same contract as the single-shape test below, asked of every window engine at once.
	// `is_new` is the only signal that survives a sweep, and each engine decides independently
	// whether to trust it, so this reports per shape rather than stopping at the first failure -
	// a fix that lands in one engine and not the others must show up as a shorter list, not a pass.
	let mut broken: Vec<String> = Vec::new();

	for (name, kind) in kinds() {
		let mut subject = harness_of(kind);
		let mut session = Session::new(&mut subject);

		session.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS))]))
			.expect("apply must succeed");
		let reclaimed = session.reclaim(SWEEP_MS).expect("sweep must succeed");
		if reclaimed.is_empty() {
			broken.push(format!("{name}: nothing was reclaimed, so the case was never reached"));
			continue;
		}

		session.apply(generator::insert(vec![generator::row(RowNumber(2), 1, 5, at(SWEEP_MS + 1))]))
			.expect("apply must succeed");

		if !session.incoherent().is_empty() {
			broken.push(format!("{name}: {:?}", session.incoherent()));
		}
	}

	assert!(broken.is_empty(), "window shapes that break the diff stream after a sweep:\n{}", broken.join("\n"));
}

#[test]
fn no_window_shape_leaves_its_state_unbounded_after_a_sweep() {
	// The anti-vacuity twin, per shape. A shape whose data never shrinks is not exercising
	// reclamation at all, so a green result above would be meaningless for it.
	let mut unbounded: Vec<String> = Vec::new();

	for (name, kind) in kinds() {
		let mut subject = harness_of(kind);
		subject.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS))]))
			.expect("apply must succeed");

		let before = subject.footprint().expect("footprint must succeed");
		subject.reclaim(SWEEP_MS).expect("sweep must succeed");
		let after = subject.footprint().expect("footprint must succeed");

		// Two different failures, and conflating them would hide the worse one. A shape holding no
		// group-scoped data at all was never exercised by this corpus, so its result in the
		// double-publish test above is vacuous - that is a gap in coverage, not a bounded operator.
		if before.data_rows == 0 {
			unbounded.push(format!(
				"{name}: holds no group-scoped data ({before:?}), so this corpus never gave a sweep \
				 anything to reach and the contract is untested for this shape"
			));
		} else if after.data_rows >= before.data_rows {
			unbounded.push(format!("{name}: a sweep did not bound it: {before:?} -> {after:?}"));
		}
	}

	assert!(unbounded.is_empty(), "window shapes the sweep did not demonstrably bound:\n{}", unbounded.join("\n"));
}

#[test]
fn a_sweep_publishes_nothing_into_the_view() {
	// Not a formality. `Session` folds whatever a subject hands back, so a sweep that emitted even
	// one diff would silently enter the view and every bound below would be checked against a table
	// no model describes. It is also the property that lets reclamation be modelled as invisible
	// rather than as an event the oracle has to predict.
	let mut subject = harness();
	let mut session = Session::new(&mut subject);

	session.apply(generator::insert(vec![
		generator::row(RowNumber(1), 1, 10, at(0)),
		generator::row(RowNumber(2), 2, 20, at(0)),
	]))
	.expect("apply must succeed");
	let before = session.view().rows.clone();

	let reclaimed = session.reclaim(SWEEP_MS).expect("sweep must succeed");

	assert!(!reclaimed.is_empty(), "precondition: the sweep must actually have retired something");
	assert_eq!(session.view().rows, before, "a sweep must not change the published view");
	assert!(session.incoherent().is_empty());
}

#[test]
fn a_group_removed_after_its_state_was_reclaimed_leaves_the_stream_foldable() {
	// The contract itself. A row arrives, its group ages out and is swept, and then the upstream
	// retracts that row - so the operator is asked to withdraw something whose state no longer
	// exists. It may leave the published row stranded; what it may not do is emit a remove for a row
	// the view never held, or an update whose pre-image is absent, because a sink applying that
	// stream has nowhere to put it.
	let mut subject = harness();
	let mut session = Session::new(&mut subject);

	let row = generator::row(RowNumber(1), 1, 10, at(0));
	session.apply(generator::insert(vec![row.clone()])).expect("apply must succeed");
	assert!(!session.reclaim(SWEEP_MS).expect("sweep must succeed").is_empty());

	session.apply(generator::remove(vec![row])).expect("apply must succeed");

	assert!(
		session.incoherent().is_empty(),
		"a retraction against reclaimed state must stay foldable: {:?}",
		session.incoherent()
	);
}

#[test]
fn a_key_that_wakes_after_reclamation_does_not_double_publish() {
	// The other half of foldability, and the one the keyed comparison could not previously see:
	// after the identity that addressed a group is gone, a woken key can mint a second output row
	// beside the one it already published. `MaterializedView::fold` reports that as an insert over a
	// row that already existed, so the assertion is on `incoherent` rather than on a count.
	let mut subject = harness();
	let mut session = Session::new(&mut subject);

	session.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");
	assert!(!session.reclaim(SWEEP_MS).expect("sweep must succeed").is_empty());

	session.apply(generator::insert(vec![generator::row(RowNumber(2), 1, 5, at(SWEEP_MS + 1))]))
		.expect("apply must succeed");

	assert!(
		session.incoherent().is_empty(),
		"a woken key must not publish a second row over its own: {:?}",
		session.incoherent()
	);
}

#[test]
fn sweeping_bounds_the_state_while_leaving_the_identity_that_addresses_it() {
	// The anti-vacuity guard for every assertion above: they would all hold trivially against a
	// sweep that deleted nothing. This measures the durable rows directly rather than trusting the
	// sweep's own report, and checks the two-phase split at the same time - the data half goes, the
	// identity half stays, because a published row still names the mapping.
	let mut subject = harness();

	subject.apply(generator::insert(vec![
		generator::row(RowNumber(1), 1, 10, at(0)),
		generator::row(RowNumber(2), 2, 20, at(0)),
	]))
	.expect("apply must succeed");

	let before = subject.footprint().expect("footprint must succeed");
	assert!(before.data_rows > 0, "precondition: the operator must be holding data to reclaim");

	let reclaimed = subject.reclaim(SWEEP_MS).expect("sweep must succeed");
	let after = subject.footprint().expect("footprint must succeed");

	assert!(!reclaimed.is_empty(), "the sweep must report what it retired");
	assert!(after.data_rows < before.data_rows, "state must actually shrink: {before:?} -> {after:?}");
	assert_eq!(
		after.identity_rows, before.identity_rows,
		"the identity half must survive the data phase, or a woken group mints a duplicate row"
	);
}

#[test]
fn the_identity_phase_reaches_a_group_one_grid_width_after_the_data_phase() {
	// Phase two, which no suite could reach at all until the harness could declare a sink row ttl.
	// The ordering asserted here is the reason reclamation is split in two: identity must outlive the
	// data it addresses, because a mapping retired under a live sink row makes the next event on that
	// key mint a second row beside the one already published.
	//
	// Both halves are load-bearing. A sweep that took identity together with data would collapse the
	// split silently, and a sweep that never took identity at all would leave every mapping in the
	// database forever while this file's other assertions still passed.
	let mut subject = harness();
	subject.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS))]))
		.expect("apply must succeed");

	let before = subject.footprint().expect("footprint must succeed");
	assert!(before.identity_rows > 0, "precondition: the operator must have minted an identity to reclaim");

	let data_only = subject.reclaim(SWEEP_MS).expect("sweep must succeed");
	let mid = subject.footprint().expect("footprint must succeed");

	assert!(!data_only.data.is_empty(), "the data phase must reach the group at its own cutoff");
	assert!(
		data_only.identity.is_empty(),
		"identity trails data by one grid width and is not due yet, but the sweep took {:?}",
		data_only.identity
	);
	assert_eq!(mid.identity_rows, before.identity_rows, "no mapping may be erased while data is still due");

	let identity = subject.reclaim(IDENTITY_SWEEP_MS).expect("sweep must succeed");
	let after = subject.footprint().expect("footprint must succeed");

	assert!(!identity.identity.is_empty(), "once the ttl clears the bucket the identity phase must reach it");
	assert!(
		after.identity_rows < before.identity_rows,
		"identity rows must actually shrink: {before:?} -> {after:?}"
	);
}

#[test]
fn a_sweep_before_the_horizon_is_a_no_op_in_every_observable_way() {
	// The control that gives the four tests above their meaning. Same corpus, same calls, a
	// watermark one millisecond inside the horizon: nothing retired, nothing shrunk, view untouched.
	// If this diverged from the reclaiming case for any reason other than the watermark, none of the
	// assertions above would be evidence about reclamation.
	let mut subject = harness();
	subject.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");

	let before = subject.footprint().expect("footprint must succeed");
	let reclaimed = subject.reclaim(EARLY_SWEEP_MS).expect("sweep must succeed");
	let after = subject.footprint().expect("footprint must succeed");

	assert_eq!(reclaimed, Reclaimed::default(), "nothing is due one millisecond early");
	assert_eq!(after, before, "and nothing is erased");
}
