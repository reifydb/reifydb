// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The foldable-forever contract for the keyed operators. The data phase erases a group's state while
//! its row-number mapping deliberately outlives it, so an operator reading "no state" as "new key"
//! publishes an Insert over a row the sink is still holding - a diff no sink can fold.

use std::sync::Arc;

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	key::encoded::EncodedKey,
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	key::operator_state::{Keyspace, OperatorStateKey, StateKey},
	value::column::columns::Columns,
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_routine::{
	function::default_native_functions, monoid::default_native_monoids, procedure::default_native_procedures,
	routine::registry::Routines,
};
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sub_flow::{
	context::FlowContext,
	operator::{
		OperatorCell, Operators, aggregation::operator::AggregateOperator, append::AppendOperator,
		apply::ApplyOperator, distinct::operator::DistinctOperator, scan::series::SourceSeriesOperator,
	},
};
use reifydb_testing_chaos::operator::session::Session;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use crate::framework::{generator, harness::Harness};

// The declared ttl every operator here carries. It is also the retention scale, so the grid is
// 60s / 16 = 3.75s and every arrival below lands in bucket zero.
const TTL_SECS: i64 = 60;

const SPAN_MS: u64 = TTL_SECS as u64 * 1_000;

const GRID_WIDTH_MS: u64 = SPAN_MS / 16;

const ARRIVAL_MS: u64 = 1_000;

// A group is due once its bucket falls STRICTLY below the cutoff's, so the cutoff has to clear the
// whole of bucket zero: `watermark - ttl >= GRID_WIDTH_MS`.
const SWEEP_MS: u64 = SPAN_MS + GRID_WIDTH_MS;

// One millisecond short, for the no-op control.
const EARLY_SWEEP_MS: u64 = SWEEP_MS - 1;

const NODE: FlowNodeId = FlowNodeId(1);

const PARENT: FlowNodeId = FlowNodeId(0);

fn ttl() -> Duration {
	Duration::from_seconds(TTL_SECS).expect("the ttl is representable")
}

fn at(ms: u64) -> DateTime {
	DateTime::from_timestamp_millis(ms).unwrap()
}

fn parent() -> OperatorCell {
	OperatorCell::new(Operators::SourceSeries(SourceSeriesOperator::new(PARENT)))
}

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_native_functions(b);
	let b = default_native_procedures(b);
	default_native_monoids(b).configure()
}

fn aggregate(runtime: RuntimeContext) -> AggregateOperator {
	AggregateOperator::new(
		parent(),
		NODE,
		parse_expression("g").expect("group_by parses"),
		parse_expression("total: math::sum(v)").expect("aggregations parse"),
		routines(),
		runtime,
		Some(ttl()),
	)
}

fn distinct(runtime: RuntimeContext) -> DistinctOperator {
	DistinctOperator::new(
		parent(),
		NODE,
		parse_expression("g").expect("the distinct key parses"),
		routines(),
		runtime,
		Arc::new(FlowContext::default()),
		Some(ttl()),
	)
}

/// Append places a diff by its origin and refuses one it cannot place, so the generator's
/// table-origin change has to be re-tagged as arriving from the input this operator is wired to.
fn from_input(change: Change) -> Change {
	Change::from_flow(PARENT, change.version, change.diffs, change.changed_at)
}

/// The identity every other operator takes, so the corpus below reads the same for all of them.
fn keep(change: Change) -> Change {
	change
}

fn append() -> AppendOperator {
	// Two inputs because append refuses fewer, but the corpus only ever feeds the first: what is
	// under test is the group's lifecycle, not the fan-in.
	AppendOperator::new(NODE, vec![parent(), parent()], vec![PARENT, PARENT], Some(ttl()))
}

/// A guest that keeps one counter per key, the shape every stateful chaindex operator has. It goes
/// through `ApplyOperator` because the wrapper is what forwards `retention_scale`,
/// `reclaimable_through` and `invalidate_groups`, each of which silently disables reclamation if lost.
struct Tally {
	node: FlowNodeId,
}

const TALLY_CAPABILITIES: &[OperatorCapability] = &[
	OperatorCapability::Insert,
	OperatorCapability::Update,
	OperatorCapability::Delete,
	OperatorCapability::Reclaim,
];

impl Tally {
	fn state_key(group: reifydb_core::key::operator_state::GroupId) -> StateKey {
		OperatorStateKey::inner_encoded(group, Keyspace::FIRST_CUSTOM, vec![])
	}

	fn tally(&self, txn: &mut FlowTransaction, post: &Columns) -> Result<Vec<Diff>> {
		let mut minted: Vec<usize> = Vec::new();
		let mut woken: Vec<usize> = Vec::new();
		let mut numbers: Vec<RowNumber> = Vec::with_capacity(post.row_count());

		for row in 0..post.row_count() {
			let key = EncodedKey::new(post.row_numbers()[row].0.to_be_bytes());
			let (group, _) = txn.intern_groups(self.node, &[key.clone()])?[0];
			let state = Self::state_key(group);
			let prior: u64 = match txn.state_get(self.node, &state)? {
				Some(row) => decode_state(&StateBytes::from_row(row)?)?,
				None => 0,
			};
			let bytes = (prior + 1).encode_state(DateTime::default())?;
			txn.state_set(self.node, &state, bytes.into_row())?;

			let (number, is_new) =
				txn.get_or_create_row_number(self.node, group, &EncodedKey::new(vec![]))?;
			numbers.push(number);
			if is_new {
				minted.push(row);
			} else {
				woken.push(row);
			}
		}

		let published = post.clone().with_row_numbers(numbers);
		let mut out = Vec::new();
		if !minted.is_empty() {
			out.push(Diff::insert(published.extract_by_indices(&minted)));
		}
		if !woken.is_empty() {
			let existing = published.extract_by_indices(&woken);
			out.push(Diff::update(existing.clone(), existing));
		}
		Ok(out)
	}
}

impl Operator for Tally {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		TALLY_CAPABILITIES
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut out = Vec::new();
		for diff in &change.diffs {
			if let Diff::Insert {
				post,
				..
			} = diff
			{
				out.extend(self.tally(txn, post)?);
			}
		}
		Ok(Change::from_flow(self.node, change.version, out, change.changed_at))
	}
}

fn guest() -> ApplyOperator {
	ApplyOperator::new(
		parent(),
		NODE,
		Box::new(Tally {
			node: NODE,
		}),
		Some(ttl()),
	)
}

/// Builds from the harness's own runtime context rather than a fresh one, so the operator reads the
/// same mock clock the sweep is driven against; a second clock would put its notion of now in a
/// different domain than the coordinates the substrate ages it on.
fn harness<O: Operator>(build: impl FnOnce(RuntimeContext) -> O) -> Harness<O> {
	// The sink ttl bounds the identity phase; without it the phase never runs and every "the identity
	// half survived" assertion would hold because it was skipped.
	Harness::new(build).with_activity_grid().with_sink_row_ttl(ttl())
}

/// Drives one key through a sweep and back, and reports what the operator broke. The key is written
/// again while the mapping the sweep deliberately left behind still names a live row, so an Insert
/// against that row is reported rather than silently overwriting it.
fn wakes_cleanly<O: Operator>(subject: &mut Harness<O>) -> Vec<String> {
	let mut broken = Vec::new();
	let mut session = Session::new(subject);

	session.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS))]))
		.expect("apply must succeed");
	let reclaimed = session.reclaim(SWEEP_MS).expect("sweep must succeed");
	if reclaimed.is_empty() {
		broken.push("nothing was reclaimed, so the case was never reached".to_string());
		return broken;
	}
	if !reclaimed.identity.is_empty() {
		broken.push(format!(
			"identity was taken on the same sweep as the data it addresses: {:?}",
			reclaimed.identity
		));
	}

	session.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 5, at(SWEEP_MS + 1))]))
		.expect("apply must succeed");

	if !session.incoherent().is_empty() {
		broken.push(format!("{:?}", session.incoherent()));
	}
	broken
}

#[test]
fn an_aggregate_key_that_wakes_after_reclamation_does_not_double_publish() {
	let mut subject = harness(aggregate);
	let broken = wakes_cleanly(&mut subject);
	assert!(broken.is_empty(), "aggregate broke the diff stream after a sweep:\n{}", broken.join("\n"));
}

#[test]
fn a_distinct_value_that_wakes_after_reclamation_does_not_double_publish() {
	let mut subject = harness(distinct);
	let broken = wakes_cleanly(&mut subject);
	assert!(broken.is_empty(), "distinct broke the diff stream after a sweep:\n{}", broken.join("\n"));
}

#[test]
fn a_guest_key_that_wakes_after_reclamation_does_not_double_publish() {
	let mut subject = harness(|_| guest());
	let broken = wakes_cleanly(&mut subject);
	assert!(broken.is_empty(), "the guest broke the diff stream after a sweep:\n{}", broken.join("\n"));
}

#[test]
fn an_append_mapping_survives_the_data_phase_and_falls_only_to_the_sweep_after_it() {
	// Append's group holds only the mapping, so its whole exposure is the identity phase and the
	// ordering is the contract: a mapping taken on the sweep that released the group retires the name
	// of a row the sink still holds, and every later mutation of it is dropped on the floor.
	let mut subject = harness(|_| append());
	let mut session = Session::new(&mut subject);

	let row = generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS));
	session.apply(from_input(generator::insert(vec![row.clone()]))).expect("apply must succeed");
	let before = session.footprint().expect("footprint must succeed").expect("the subject reports a footprint");
	assert!(before.identity_rows > 0, "precondition: the source row must have minted a mapping");

	let released = session.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert!(released.identity.is_empty(), "the data phase must run first, even with no data to erase");
	assert_eq!(
		session.footprint()
			.expect("footprint must succeed")
			.expect("the subject reports a footprint")
			.identity_rows,
		before.identity_rows,
		"no mapping may be erased on the sweep that releases the group"
	);

	// The same watermark deliberately: what makes the group reachable now is that the previous sweep
	// marked it released, not that the clock moved.
	let identity = session.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert!(!identity.identity.is_empty(), "the next sweep must take the mapping");
	assert!(
		session.footprint()
			.expect("footprint must succeed")
			.expect("the subject reports a footprint")
			.identity_rows < before.identity_rows,
		"identity rows must actually shrink, or this proves nothing about the second phase"
	);

	// The mutation the reclaimed mapping can no longer name. Append drops it rather than translating
	// it against a mapping it does not have, which is the only answer that keeps the stream foldable.
	session.apply(from_input(generator::remove(vec![row]))).expect("apply must succeed");
	assert!(
		session.incoherent().is_empty(),
		"a mutation whose mapping was reclaimed must be dropped, not published: {:?}",
		session.incoherent()
	);
}

#[test]
fn a_retraction_against_reclaimed_state_stays_foldable_for_every_keyed_operator() {
	// The other half of the contract, and the one an operator fails by over-publishing rather than
	// under-publishing: asked to withdraw a row whose state is gone, it may strand the row, but it
	// may not emit a remove for a row the view never held or an update whose pre-image is absent.
	let mut broken: Vec<String> = Vec::new();

	macro_rules! check {
		($name:literal, $operator:expr, $tag:expr) => {{
			let mut subject = harness($operator);
			let mut session = Session::new(&mut subject);
			let row = generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS));
			session.apply($tag(generator::insert(vec![row.clone()]))).expect("apply must succeed");
			session.reclaim(SWEEP_MS).expect("sweep must succeed");
			session.apply($tag(generator::remove(vec![row]))).expect("apply must succeed");
			if !session.incoherent().is_empty() {
				broken.push(format!("{}: {:?}", $name, session.incoherent()));
			}
		}};
	}

	check!("aggregate", aggregate, keep);
	check!("distinct", distinct, keep);
	check!("append", |_| append(), from_input);
	check!("guest", |_| guest(), keep);

	assert!(broken.is_empty(), "retractions that broke the stream after a sweep:\n{}", broken.join("\n"));
}

#[test]
fn a_sweep_publishes_nothing_into_the_view_for_every_keyed_operator() {
	// `Session` folds whatever a subject hands back, so a sweep that emitted even one diff would
	// silently enter the view and every bound above would be checked against a table no model
	// describes.
	let mut broken: Vec<String> = Vec::new();

	macro_rules! check {
		($name:literal, $operator:expr, $tag:expr) => {{
			let mut subject = harness($operator);
			let mut session = Session::new(&mut subject);
			session.apply($tag(generator::insert(vec![
				generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS)),
				generator::row(RowNumber(2), 2, 20, at(ARRIVAL_MS)),
			])))
			.expect("apply must succeed");
			let before = session.view().rows.clone();
			let reclaimed = session.reclaim(SWEEP_MS).expect("sweep must succeed");
			if reclaimed.is_empty() {
				broken.push(format!("{}: nothing was reclaimed, so the case was never reached", $name));
			} else if session.view().rows != before {
				broken.push(format!("{}: a sweep changed the published view", $name));
			}
		}};
	}

	check!("aggregate", aggregate, keep);
	check!("distinct", distinct, keep);
	check!("append", |_| append(), from_input);
	check!("guest", |_| guest(), keep);

	assert!(broken.is_empty(), "sweeps that were not invisible to the view:\n{}", broken.join("\n"));
}

#[test]
fn sweeping_bounds_every_keyed_operator_while_leaving_the_identity_that_addresses_it() {
	// The anti-vacuity guard for everything above, all of which holds trivially against a sweep that
	// deleted nothing. Append is left out because it holds no data at all, which keeps "bounded"
	// distinct from "never had anything to bound".
	let mut broken: Vec<String> = Vec::new();

	macro_rules! check {
		($name:literal, $operator:expr) => {{
			let mut subject = harness($operator);
			subject.apply(generator::insert(vec![
				generator::row(RowNumber(1), 1, 10, at(ARRIVAL_MS)),
				generator::row(RowNumber(2), 2, 20, at(ARRIVAL_MS)),
			]))
			.expect("apply must succeed");
			// Append is absent from this list by construction; see the comment above.
			let before = subject.footprint().expect("footprint must succeed");
			subject.reclaim(SWEEP_MS).expect("sweep must succeed");
			let after = subject.footprint().expect("footprint must succeed");
			if before.data_rows == 0 {
				broken.push(format!(
					"{}: holds no group-scoped data ({before:?}), so this corpus never gave the \
					 data phase anything to reach",
					$name
				));
			} else if after.data_rows >= before.data_rows {
				broken.push(format!("{}: a sweep did not bound it: {before:?} -> {after:?}", $name));
			} else if after.identity_rows != before.identity_rows {
				broken.push(format!(
					"{}: the identity half must survive the data phase: {before:?} -> {after:?}",
					$name
				));
			}
		}};
	}

	check!("aggregate", aggregate);
	check!("distinct", distinct);
	check!("guest", |_| guest());

	assert!(broken.is_empty(), "operators the sweep did not demonstrably bound:\n{}", broken.join("\n"));
}

#[test]
fn a_sweep_one_millisecond_before_the_cutoff_clears_the_bucket_is_a_no_op() {
	// The control that gives every test above its meaning: same corpus and calls with a watermark one
	// millisecond short of clearing the bucket. If this diverged for any reason other than the cutoff,
	// none of the assertions above would be evidence about reclamation.
	let mut broken: Vec<String> = Vec::new();

	macro_rules! check {
		($name:literal, $operator:expr, $tag:expr) => {{
			let mut subject = harness($operator);
			subject.apply($tag(generator::insert(vec![generator::row(
				RowNumber(1),
				1,
				10,
				at(ARRIVAL_MS),
			)])))
			.expect("apply must succeed");
			let before = subject.footprint().expect("footprint must succeed");
			let reclaimed = subject.reclaim(EARLY_SWEEP_MS).expect("sweep must succeed");
			let after = subject.footprint().expect("footprint must succeed");
			if !reclaimed.is_empty() {
				broken.push(format!("{}: nothing is due one millisecond early: {reclaimed:?}", $name));
			} else if reclaimed.cutoffs.data != Some(EARLY_SWEEP_MS - SPAN_MS) {
				broken.push(format!(
					"{}: the data phase must still have RUN at its own cutoff, but reported {:?} - \
					 a phase that was skipped retires nothing either",
					$name, reclaimed.cutoffs.data
				));
			} else if after != before {
				broken.push(format!("{}: nothing may be erased: {before:?} -> {after:?}", $name));
			}
		}};
	}

	check!("aggregate", aggregate, keep);
	check!("distinct", distinct, keep);
	check!("append", |_| append(), from_input);
	check!("guest", |_| guest(), keep);

	assert!(broken.is_empty(), "sweeps that were not no-ops one millisecond early:\n{}", broken.join("\n"));
}
