// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The foldable-forever contract, driven against a real sweep. Reclamation deletes operator state and
//! publishes no diffs, so the promise is not "the view stays correct" but "whatever the sweep deletes,
//! the operator never afterwards publishes a diff a sink cannot apply": stale rows yes, bad diffs no.

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_sub_flow::operator::window::operator::WindowOperator;
use reifydb_testing_chaos::operator::{session::Session, subject::Subject};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

use crate::{
	framework::{generator, harness::Harness},
	operators::window::{WindowSpec, build},
};

// Every shape below is sized at this, and with zero grace it is the horizon too. Not free to pick:
// an operator's own seal span overrides whatever the harness declares.
const WINDOW_SECS: i64 = 60;

const SPAN_MS: u64 = WINDOW_SECS as u64 * 1_000;

// Sixteen buckets per horizon, so a 60s span grids at 3.75s.
const GRID_WIDTH_MS: u64 = SPAN_MS / 16;

// One second past the epoch, which puts every arrival in grid bucket zero - the bucket the cutoff
// assertions below are written against.
const ARRIVAL_MS: u64 = 1_000;

// Nothing an operator has not sealed is reclaimable, and a seal at T only proves windows anchored at
// or before `T - span - 1` are closed - so with every arrival here in bucket zero, the anchor has to
// reach one full grid width.
const SEAL_MS: u64 = SPAN_MS + GRID_WIDTH_MS + 1;

// One millisecond short: a group is due only once its bucket falls strictly below the cutoff's.
const EARLY_SEAL_MS: u64 = SEAL_MS - 1;

// The identity phase is the one cutoff measured from the sweep watermark, because it belongs to the
// sink: a mapping has to outlive the published row naming it, and that row lives exactly the sink's
// row ttl.
const SWEEP_MS: u64 = SPAN_MS + GRID_WIDTH_MS;

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
	// The sink ttl bounds the identity phase; without it the phase never runs and every "the identity
	// half survived" assertion here would hold because it was skipped.
	let span = Duration::from_seconds(WINDOW_SECS).expect("span is representable");
	Harness::new(|runtime| build(&spec(), runtime)).with_activity_grid().with_sink_row_ttl(span)
}

fn at(ms: u64) -> DateTime {
	DateTime::from_timestamp_millis(ms).unwrap()
}

fn kinds() -> Vec<(&'static str, WindowKind)> {
	// Separate engines with separate emit paths, each deciding Insert-versus-Update independently, so
	// a fix in one says nothing about the others.
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
	// The sink ttl bounds the identity phase; without it the phase never runs and every "the identity
	// half survived" assertion here would hold because it was skipped.
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
	// `is_new` is the only signal that survives a sweep and each engine decides independently whether
	// to trust it, so this reports per shape rather than stopping at the first - a fix that lands in
	// one engine must show up as a shorter list, not a pass.
	let mut broken: Vec<String> = Vec::new();

	for (name, kind) in kinds() {
		let mut subject = harness_of(kind);
		let mut session = Session::new(&mut subject);

		session.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS))]))
			.expect("apply must succeed");
		session.tick(SEAL_MS).expect("seal must succeed");
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
		subject.tick(SEAL_MS).expect("seal must succeed");
		subject.reclaim(SWEEP_MS).expect("sweep must succeed");
		let after = subject.footprint().expect("footprint must succeed");

		// A shape holding no group-scoped data was never exercised by this corpus, so its result
		// above is vacuous - a gap in coverage, not a bounded operator.
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
	// `Session` folds whatever a subject hands back, so a sweep that emitted even one diff would
	// silently enter the view and every bound below would be checked against a table no model
	// describes.
	let mut subject = harness();
	let mut session = Session::new(&mut subject);

	session.apply(generator::insert(vec![
		generator::row(RowNumber(1), 1, 10, at(0)),
		generator::row(RowNumber(2), 2, 20, at(0)),
	]))
	.expect("apply must succeed");
	let before = session.view().rows.clone();

	session.tick(SEAL_MS).expect("seal must succeed");
	let reclaimed = session.reclaim(SWEEP_MS).expect("sweep must succeed");

	assert!(!reclaimed.is_empty(), "precondition: the sweep must actually have retired something");
	assert_eq!(session.view().rows, before, "a sweep must not change the published view");
	assert!(session.incoherent().is_empty());
}

#[test]
fn a_group_removed_after_its_state_was_reclaimed_leaves_the_stream_foldable() {
	// The contract itself: the operator is asked to withdraw a row whose state no longer exists. It
	// may leave the published row stranded; it may not emit a remove for a row the view never held or
	// an update whose pre-image is absent, because a sink has nowhere to put that.
	let mut subject = harness();
	let mut session = Session::new(&mut subject);

	let row = generator::row(RowNumber(1), 1, 10, at(0));
	session.apply(generator::insert(vec![row.clone()])).expect("apply must succeed");
	session.tick(SEAL_MS).expect("seal must succeed");
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
	// The other half of foldability: once the identity addressing a group is gone, a woken key can
	// mint a second output row beside the one it already published. `fold` reports that as an insert
	// over an existing row, so the assertion is on `incoherent` rather than on a count.
	let mut subject = harness();
	let mut session = Session::new(&mut subject);

	session.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");
	session.tick(SEAL_MS).expect("seal must succeed");
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
	// The anti-vacuity guard for every assertion above, which would all hold trivially against a
	// sweep that deleted nothing. Measures the durable rows directly rather than trusting the sweep's
	// own report, and checks the split: the data half goes, the identity half stays.
	let mut subject = harness();

	subject.apply(generator::insert(vec![
		generator::row(RowNumber(1), 1, 10, at(0)),
		generator::row(RowNumber(2), 2, 20, at(0)),
	]))
	.expect("apply must succeed");

	let before = subject.footprint().expect("footprint must succeed");
	assert!(before.data_rows > 0, "precondition: the operator must be holding data to reclaim");

	subject.tick(SEAL_MS).expect("seal must succeed");
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
fn the_identity_phase_reaches_a_group_only_on_a_sweep_after_the_data_phase() {
	// Identity must outlive the data it addresses: a mapping retired under a live sink row makes the
	// next event on that key mint a second row beside the one already published. The ordering is
	// structural - identity only reaches groups the data phase has already finished and marked.
	let mut subject = harness();
	subject.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS))]))
		.expect("apply must succeed");

	let before = subject.footprint().expect("footprint must succeed");
	assert!(before.identity_rows > 0, "precondition: the operator must have minted an identity to reclaim");

	subject.tick(SEAL_MS).expect("seal must succeed");
	let data_only = subject.reclaim(SWEEP_MS).expect("sweep must succeed");
	let mid = subject.footprint().expect("footprint must succeed");

	assert!(!data_only.data.is_empty(), "the data phase must reach the group at its own cutoff");
	assert!(
		data_only.identity.is_empty(),
		"identity may not be taken on the sweep that reclaims the data it addresses, but the sweep \
		 took {:?}",
		data_only.identity
	);
	assert_eq!(mid.identity_rows, before.identity_rows, "no mapping may be erased while data is still due");

	// The same watermark, deliberately: what makes the group reachable now is that the previous sweep
	// marked its data reclaimed, not that the clock moved.
	let identity = subject.reclaim(SWEEP_MS).expect("sweep must succeed");
	let after = subject.footprint().expect("footprint must succeed");

	assert!(
		!identity.identity.is_empty(),
		"once the data phase has finished with a group the next sweep must take its identity"
	);
	assert!(
		after.identity_rows < before.identity_rows,
		"identity rows must actually shrink: {before:?} -> {after:?}"
	);
}

#[test]
fn a_sweep_before_the_seal_clears_the_bucket_is_a_no_op_in_every_observable_way() {
	// The control that gives the four tests above their meaning: same corpus and calls with a seal one
	// millisecond short of clearing the bucket. "No-op" is pinned as "the phases ran and found nothing
	// due" - a sweep that skipped the node also retires nothing, and the cutoffs tell the two apart.
	let mut subject = harness();
	subject.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");

	let before = subject.footprint().expect("footprint must succeed");
	subject.tick(EARLY_SEAL_MS).expect("seal must succeed");
	let reclaimed = subject.reclaim(SWEEP_MS).expect("sweep must succeed");
	let after = subject.footprint().expect("footprint must succeed");

	assert!(reclaimed.is_empty(), "nothing is due one millisecond early, but got {reclaimed:?}");
	assert_eq!(reclaimed.rows, 0, "and no rows were touched");
	assert_eq!(
		reclaimed.cutoffs.data,
		Some(EARLY_SEAL_MS - SPAN_MS - 1),
		"the data phase must still have RUN, at the anchor the early seal proves - a phase that was \
		 skipped retires nothing either, and this is what separates the two"
	);
	assert!(
		reclaimed.cutoffs.identity.is_some(),
		"the identity phase runs off the sink row ttl rather than the seal, so it is reached whatever \
		 the ledger says"
	);
	assert_eq!(after, before, "and nothing is erased");
}
