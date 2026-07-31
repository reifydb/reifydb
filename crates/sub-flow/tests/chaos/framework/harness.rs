// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_abi::operator::timer::TimerKind;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers},
	common::CommitVersion,
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::{EncodableKey, flow_node_state::FlowNodeStateKey, operator_state::OperatorStateKey},
	state::{
		budget::OperatorStateBudgetHandle,
		group::ActivityBuckets,
		horizon::{Cutoff, activity_buckets},
	},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_flow::{
	operator::Operator,
	timer::Timer,
	transaction::{ChangeCoordinate, DeferredParams, FlowTransaction, substrate::FlowSubstrate},
};
use reifydb_runtime::context::{
	RuntimeContext,
	clock::{Clock, MockClock},
};
use reifydb_sub_flow::execution::reclaim::{PhaseReclaim, ReclaimBudget, ReclaimReport, SweepInputs, reclaim_nodes};
use reifydb_testing_chaos::operator::{
	reclaim::{Reclaimed, RetiredGroup, StateFootprint},
	subject::Subject,
};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use crate::framework::generator;

pub struct Harness<O: Operator> {
	engine: TestEngine,
	operator: O,
	clock: MockClock,
	version: u64,
	pending: Pending,
	substrate: FlowSubstrate,
	sink_row_ttl: Option<Duration>,
	reclaim_budget: ReclaimBudget,
	mapping_cursor: Option<EncodedKey>,
}

impl<O: Operator> Harness<O> {
	pub fn new(build: impl FnOnce(RuntimeContext) -> O) -> Self {
		Self::with_engine(|_, runtime| build(runtime))
	}

	/// For operators whose constructor needs more of the engine than a runtime context - a join takes
	/// an `Executor`, and it has to be this harness's engine or the operator would evaluate its key
	/// expressions against a different catalog and clock than the one driving it.
	pub fn with_engine(build: impl FnOnce(&TestEngine, RuntimeContext) -> O) -> Self {
		let engine = TestEngine::new();
		let clock = engine.mock_clock();
		let runtime = RuntimeContext::new(
			Clock::Mock(clock.clone()),
			engine.inner().rng().clone(),
			engine.inner().version_epoch().clone(),
		);
		let operator = build(&engine, runtime);
		Self {
			engine,
			operator,
			clock,
			version: 1,
			pending: Pending::new(),
			substrate: FlowSubstrate::new(),
			sink_row_ttl: None,
			reclaim_budget: ReclaimBudget {
				groups: 256,
				rows: 1_024,
			},
			mapping_cursor: None,
		}
	}

	/// Registers the activity grid a reclaim sweep will read this node's stamps against.
	///
	/// Production does this once per node at flow registration (`adopt_horizon`), and a harness that
	/// skips it does not merely lose precision: the node falls back to `ActivityBuckets::undeclared`,
	/// whose `event_grid()` is `None`, which is the condition the reclaim driver silently skips a
	/// node on. So without this call a sweep is not inaccurate, it does not happen at all.
	///
	/// The grid comes from the operator's own `retention_scale`, exactly as production's
	/// `adopt_horizon` does. The harness takes no span of its own: a suite that gridded on a number
	/// it chose would be asserting against a node configuration the engine cannot register - a 60s
	/// window declared at 16s reclaims on a cutoff and a slack that are both 3.75x off what ships.
	///
	/// Opt-in rather than automatic, because it is only meaningful for a suite that also gives its
	/// rows real event times - see `coordinate_of`.
	pub fn with_activity_grid(self) -> Self {
		self.substrate.group.set_activity_grid(self.operator.id(), self.operator.retention_scale());
		self
	}

	/// Declares the row ttl of the sink this operator publishes into, which is what bounds the
	/// identity phase.
	///
	/// Without it `identity_span` is `None` and `reclaim_nodes` skips phase two entirely, so the
	/// mapping a published row still names is retained forever. That is the safe direction, but it
	/// also means a harness that never calls this cannot exercise the hazard the two-phase split
	/// exists for: an identity retired under a live sink row makes the next event on that key mint a
	/// second row beside it.
	///
	/// Production derives this from the flow's one sink via `find_row_settings_latest`. The harness
	/// drives a bare operator with no flow behind it, so the ttl has to be stated rather than looked
	/// up.
	pub fn with_sink_row_ttl(mut self, ttl: Duration) -> Self {
		self.sink_row_ttl = Some(ttl);
		self
	}

	/// Bounds every sweep this harness runs, so a scenario can drive partial reclamation.
	///
	/// The production default is 256 groups and 1024 rows per tick, which no chaos run comes close
	/// to; a suite that wants to exercise a truncated sweep has to say so.
	pub fn with_reclaim_budget(mut self, budget: ReclaimBudget) -> Self {
		self.reclaim_budget = budget;
		self
	}

	/// The grid this node's activity stamps are bucketed on, as the reclaim driver would read it.
	pub fn activity_grid(&self) -> ActivityBuckets {
		self.substrate.group.buckets(self.operator.id())
	}

	/// Counts the operator state this node is actually holding, split into the halves reclamation
	/// treats differently.
	///
	/// A key whose inner encoding does not decode is counted as data rather than skipped: a row the
	/// substrate cannot frame is exactly the kind of leak this exists to notice, and silently
	/// dropping it from the count would hide it.
	pub fn footprint(&mut self) -> Result<StateFootprint> {
		let node = self.operator.id();
		let mut txn = self.begin(DateTime::default());
		let batch = txn.state_range(node, EncodedKeyRange::all(), None)?;
		let mut footprint = StateFootprint::default();
		for item in &batch.items {
			let decoded = FlowNodeStateKey::decode(&item.key)
				.and_then(|state| OperatorStateKey::decode_inner(&state.key));
			match decoded {
				Some((group, keyspace, _)) if keyspace.is_identity() => footprint.identity_rows += 1,
				Some((group, _, _)) if group.is_node_scope() => footprint.node_scoped_data_rows += 1,
				_ => footprint.data_rows += 1,
			}
		}
		self.end(txn);
		Ok(footprint)
	}

	fn begin(&mut self, at: DateTime) -> FlowTransaction {
		let query = self.engine.multi().begin_query().expect("begin_query");
		let state_query = self.engine.multi().begin_query().expect("begin_query");
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: CommitVersion(self.version),
			pending: mem::take(&mut self.pending),
			base_pending: PendingLayers::empty(),
			query,
			state_query,
			single: self.engine.inner().single().clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(self.clock.clone()),
			substrate: self.substrate.clone(),
			state_budget: OperatorStateBudgetHandle::default(),
		});
		txn.set_change_coordinate(ChangeCoordinate {
			at,
			version: CommitVersion(self.version),
		});
		txn
	}

	fn end(&mut self, mut txn: FlowTransaction) {
		self.pending = txn.take_pending();
		self.version += 1;
	}

	pub fn apply(&mut self, change: Change) -> Result<Change> {
		let at = coordinate_of(&change);
		let mut txn = self.begin(at);
		let out = self.operator.apply(&mut txn, change)?;
		txn.flush_operator_states()?;
		self.end(txn);
		Ok(out)
	}

	pub fn on_timer(&mut self, timer: Timer) -> Result<Option<Change>> {
		let mut txn = self.begin(timer.at);
		let out = self.operator.on_timer(&mut txn, timer)?;
		txn.flush_operator_states()?;
		self.end(txn);
		Ok(out)
	}

	/// Drives the real per-node sweep at `at_ms`, deriving its cutoffs the way the engine does.
	///
	/// This calls production's own `reclaim_nodes` on inputs built the same way `reclaim_flow`
	/// builds them, so the phase order, the shared budget and the early stop are the ones that
	/// ship. What stays harness-local is only the *ingredients*: which node, and which sink row ttl.
	///
	/// The frontier is the operator's own answer, so a windowed operator is bounded by its seal
	/// ledger here for exactly the reason it is in production - the harness no longer has to
	/// reproduce a node-type test it could never satisfy.
	pub fn reclaim(&mut self, at_ms: u64) -> Result<Reclaimed> {
		let node = self.operator.id();
		if self.substrate.group.buckets(node).event_grid().is_none() {
			return Ok(Reclaimed::default());
		}
		let watermark = DateTime::from_timestamp_millis(at_ms)?;

		let mut txn = self.begin(watermark);
		let reclaimable = self.operator.reclaimable_through(&mut txn, watermark)?;

		let inputs = SweepInputs {
			node,
			data: reclaimable.data.map(Cutoff),
			identity: self.sink_row_ttl.map(|span| Cutoff(watermark.saturating_sub(span))),
			keyspaces: reclaimable
				.keyspaces
				.into_iter()
				.map(|(keyspace, at)| (keyspace, Cutoff(at)))
				.collect(),
			mapping: reclaimable.mapping.map(Cutoff),
			mapping_cursor: self.mapping_cursor.take(),
		};

		let mut budget = self.reclaim_budget;
		let mut report = ReclaimReport::default();
		let operator = &self.operator;
		let outcome = reclaim_nodes(vec![inputs], &mut txn, &mut budget, &mut report, &mut |_, groups| {
			operator.invalidate_groups(&groups);
		})?;
		txn.flush_operator_states()?;
		self.end(txn);
		self.mapping_cursor = outcome.cursors.into_iter().next().and_then(|(_, cursor)| cursor);

		Ok(reclaimed_from(&report, node))
	}
}

/// Restates the sweep's own report in the terms a chaos oracle checks.
///
/// Every group and every cutoff here comes from the report rather than from anything the harness
/// derived, which is the point: an oracle that re-derived the cutoff would be checking the sweep
/// against a copy of the sweep's arithmetic and could never catch a resolver defect.
fn reclaimed_from(report: &ReclaimReport, node: FlowNodeId) -> Reclaimed {
	let Some(reclaim) = report.node(node) else {
		return Reclaimed {
			rows: report.rows,
			backlog: report.backlog,
			..Default::default()
		};
	};
	let retired = |phase: &PhaseReclaim| {
		phase.groups
			.iter()
			.map(|group| RetiredGroup {
				group: group.0,
				cutoff_ms: phase.cutoff.instant().to_millis(),
			})
			.collect::<Vec<_>>()
	};

	Reclaimed {
		data: reclaim.data.as_ref().map(retired).unwrap_or_default(),
		identity: reclaim.identity.as_ref().map(retired).unwrap_or_default(),
		keyspace: reclaim
			.keyspaces
			.iter()
			.flat_map(|keyspace| {
				keyspace.groups.iter().map(|group| RetiredGroup {
					group: group.0,
					cutoff_ms: keyspace.cutoff.instant().to_millis(),
				})
			})
			.collect(),
		mapping_rows: reclaim.mapping.map(|mapping| mapping.rows).unwrap_or_default(),
		rows: report.rows,
		backlog: report.backlog,
	}
}

/// The event position an applied change is stamped at.
///
/// The substrate stamps a group's activity from the transaction's change coordinate, and in
/// production that coordinate is the arrival frontier the batch path derives from the rows
/// themselves. Reading the harness clock instead pins every group of a run into one activity bucket,
/// so no group can ever fall behind a cutoff and reclamation is unreachable by construction - which
/// is what it was.
///
/// The fold mirrors sub-flow's own `max_input_time`, which is crate-private and so cannot be shared
/// with an integration test. It is three lines and pinned by `the_coordinate_is_the_latest_row_time`.
///
/// A change carrying no row time falls back to its `changed_at`. That is the honest answer rather
/// than a fresh clock read: a workload whose rows have no time has declared no event position, and a
/// suite that wants ageing has to give its rows one.
fn coordinate_of(change: &Change) -> DateTime {
	change.diffs
		.iter()
		.filter_map(|diff| diff.post().or_else(|| diff.pre()))
		.flat_map(|columns| columns.time().iter().copied())
		.max()
		.unwrap_or(change.changed_at)
}

#[test]
fn the_coordinate_is_the_latest_row_time() {
	// A batch is one arrival, so its position is the latest event time it carries - matching what
	// the batch path freezes as the arrival frontier. Taking the first row's time instead would make
	// a group's due-ness depend on how the driver happened to order rows inside a change.
	let early = DateTime::from_timestamp_millis(1_000).unwrap();
	let late = DateTime::from_timestamp_millis(9_000).unwrap();

	let change = change_at(&[early, late, early]);
	assert_eq!(coordinate_of(&change), late);

	// No row time is not the same as time zero: it means the workload declared no position, and the
	// change's own stamp is the only honest answer left.
	let stamped = DateTime::from_timestamp_millis(4_242).unwrap();
	let timeless = Change::from_flow(FlowNodeId(1), CommitVersion(1), Vec::new(), stamped);
	assert_eq!(coordinate_of(&timeless), stamped);
}

#[test]
fn a_group_falls_due_one_grid_width_after_its_span_elapses() {
	// The arithmetic every reclaim suite's coverage rests on, worked end to end so the numbers are
	// checked rather than assumed. Two unit systems meet here and neither is visible at the call
	// site: a horizon is declared as a Duration, the grid divides nanoseconds, and the chaos driver
	// speaks milliseconds. A suite that guessed wrong would simply never make a group due, and would
	// pass while asserting nothing.
	let span = Duration::from_seconds(16).expect("16s is representable");
	let grid = activity_buckets(Some(span)).event_grid().expect("a declared span buckets in event time");

	// Sixteen buckets per horizon, so a 16s span grids at 1s.
	assert_eq!(grid.width(), Duration::from_seconds(1).unwrap());

	let at = |ms: u64| DateTime::from_timestamp_millis(ms).unwrap();

	// A group stamped anywhere inside the first second reads as active at the bucket start, which is
	// the whole reason the sweep subtracts a width of slack before trusting a cutoff.
	assert_eq!(grid.of(at(0)), 0);
	assert_eq!(grid.of(at(999)), 0);
	assert_eq!(grid.of(at(1_000)), 1);

	// A group is due when its bucket is strictly below the cutoff's. The data phase cuts at
	// watermark - span, so a group in bucket 0 survives until the watermark passes 17s.
	assert_eq!(grid.first_live(at(16_999 - 16_000)), 0, "bucket 0 is not yet behind the cutoff");
	assert_eq!(grid.first_live(at(17_000 - 16_000)), 1, "at 17s the cutoff has moved past bucket 0");

	// The keyspace and mapping phases additionally subtract one width of slack, so they reach the
	// same group one full bucket later.
	assert_eq!(grid.first_live(at(17_999 - 16_000 - 1_000)), 0);
	assert_eq!(grid.first_live(at(18_000 - 16_000 - 1_000)), 1);
}

fn change_at(times: &[DateTime]) -> Change {
	// The event time lives on the encoded row, not on Columns, so this has to go through the same
	// builder the window workload uses rather than assembling Columns directly.
	generator::insert(
		times.iter()
			.enumerate()
			.map(|(index, at)| generator::row(RowNumber(index as u64 + 1), 1, index as i64, *at))
			.collect(),
	)
}

impl<O: Operator> Subject for Harness<O> {
	fn apply(&mut self, change: Change) -> Result<Change> {
		Harness::apply(self, change)
	}

	fn reclaim(&mut self, at_ms: u64) -> Result<Reclaimed> {
		Harness::reclaim(self, at_ms)
	}

	fn footprint(&mut self) -> Result<Option<StateFootprint>> {
		Harness::footprint(self).map(Some)
	}

	fn tick(&mut self, at_ms: u64) -> Result<Option<Change>> {
		// The host wheel fires on a key, but a seal is node-scoped: an empty key is what the window
		// operator arms and what the real wheel hands back.
		Harness::on_timer(
			self,
			Timer {
				at: DateTime::from_timestamp_millis(at_ms).unwrap(),
				kind: TimerKind::Seal,
				key: EncodedKey::new(Vec::new()),
			},
		)
	}
}
