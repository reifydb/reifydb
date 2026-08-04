// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use reifydb_abi::operator::{capabilities::OperatorCapability, timer::TimerKind};
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers},
	common::CommitVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId},
		change::{Change, Diff},
	},
	key::{EncodableKey, operator_group_state::OperatorGroupStateKey, operator_state::OperatorStateKey},
	state::{budget::OperatorStateBudgetHandle, group::ActivityBuckets, horizon::Cutoff},
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
use reifydb_sdk::{config::Config, operator::OperatorLogic};
use reifydb_sub_flow::{
	execution::reclaim::{
		KeyspaceCursors, PhaseReclaim, ReclaimBudget, ReclaimReport, SweepInputs, SweepOutcome, reclaim_nodes,
	},
	operator::{
		OperatorCell,
		apply::ApplyOperator,
		native::{NativeBridgedOperator, NativeOperatorAdapter},
		scan::series::SourceSeriesOperator,
	},
};
use reifydb_testing_chaos::operator::{
	reclaim::{PhaseCutoffs, Reclaimed, RetiredGroup, StateFootprint},
	subject::Subject,
};
use reifydb_transaction::{
	dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
	interceptor::interceptors::Interceptors,
};
use reifydb_value::{
	Result,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId},
};

pub struct Harness<O: Operator> {
	engine: TestEngine,
	operator: O,
	clock: MockClock,
	version: u64,
	pending: Pending,
	substrate: FlowSubstrate,
	catalog: Catalog,
	sink_row_ttl: Option<Duration>,
	reclaim_budget: ReclaimBudget,
	mapping_cursor: Option<EncodedKey>,
	keyspace_cursors: KeyspaceCursors,
}

impl<O: Operator> Harness<O> {
	pub fn new(build: impl FnOnce(RuntimeContext) -> O) -> Self {
		Self::with_engine(|_, runtime| build(runtime))
	}

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
			catalog: Catalog::testing(),
			sink_row_ttl: None,
			reclaim_budget: ReclaimBudget {
				groups: 256,
				rows: 1_024,
			},
			mapping_cursor: None,
			keyspace_cursors: KeyspaceCursors::new(),
		}
	}
}

impl Harness<ApplyOperator> {
	pub fn guest<C: OperatorLogic + 'static>(
		logic: C,
		operator: OperatorId,
		capabilities: &'static [OperatorCapability],
		ttl: Option<Duration>,
	) -> Self {
		Self::new(|_| {
			let bridged = NativeBridgedOperator::new(
				Box::new(NativeOperatorAdapter::new(logic, operator, capabilities)),
				operator,
				capabilities,
			);
			ApplyOperator::new(
				OperatorCell::new(SourceSeriesOperator::new(OperatorId(0))),
				operator,
				Box::new(bridged),
				ttl,
			)
		})
	}

	pub fn guest_from_config<C: OperatorLogic + 'static>(
		operator: OperatorId,
		capabilities: &'static [OperatorCapability],
		config: Vec<(&str, Value)>,
		ttl: Option<Duration>,
	) -> Result<Self> {
		let config = Config::new("operator", config.into_iter().map(|(k, v)| (k.to_string(), v)).collect());
		Ok(Self::guest(C::create(operator, &config)?, operator, capabilities, ttl))
	}
}

impl<O: Operator> Harness<O> {
	pub fn with_activity_grid(self) -> Self {
		self.substrate.group.set_activity_grid(self.operator.id(), self.operator.retention_scale());
		self
	}

	pub fn with_dictionaries(mut self) -> Self {
		self.catalog = self.engine.inner().catalog().clone();
		let single = self.engine.begin_admin(IdentityId::system()).expect("begin admin").single.clone();
		let registry = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single)));
		self.substrate = FlowSubstrate::with_dictionary(registry);
		self
	}

	pub fn dictionary_registry(&self) -> DictionaryAllocatorRegistry {
		let single = self.engine.begin_admin(IdentityId::system()).expect("begin admin").single.clone();
		DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single)))
	}

	pub fn engine(&self) -> &TestEngine {
		&self.engine
	}

	pub fn with_sink_row_ttl(mut self, ttl: Duration) -> Self {
		self.sink_row_ttl = Some(ttl);
		self
	}

	pub fn with_reclaim_budget(mut self, budget: ReclaimBudget) -> Self {
		self.reclaim_budget = budget;
		self
	}

	pub fn activity_grid(&self) -> ActivityBuckets {
		self.substrate.group.buckets(self.operator.id())
	}

	pub fn footprint(&mut self) -> Result<StateFootprint> {
		let operator = self.operator.id();
		let mut txn = self.begin(DateTime::default());
		let batch = txn.state_range(operator, EncodedKeyRange::all(), None)?;
		let mut footprint = StateFootprint::default();
		for item in &batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.and_then(|state| OperatorGroupStateKey::decode_inner(&state.key));
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
			catalog: self.catalog.clone(),
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

	pub fn apply_emitting(&mut self, change: Change) -> Result<Vec<(ObjectId, Diff)>> {
		let at = coordinate_of(&change);
		let mut txn = self.begin(at);
		self.operator.apply(&mut txn, change)?;
		txn.flush_operator_states()?;
		let emitted = txn.take_accumulator_entries();
		self.end(txn);
		Ok(emitted)
	}

	pub fn on_timer(&mut self, timer: Timer) -> Result<Option<Change>> {
		let mut txn = self.begin(timer.at);
		let out = self.operator.on_timer(&mut txn, timer)?;
		txn.flush_operator_states()?;
		self.end(txn);
		Ok(out)
	}

	pub fn reclaim(&mut self, at_ms: u64) -> Result<Reclaimed> {
		let operator_id = self.operator.id();
		if self.substrate.group.buckets(operator_id).event_grid().is_none() {
			return Ok(Reclaimed::default());
		}
		let watermark = DateTime::from_timestamp_millis(at_ms)?;

		let mut txn = self.begin(watermark);
		let reclaimable = self.operator.reclaimable_through(&mut txn, watermark)?;

		let inputs = SweepInputs {
			operator: operator_id,
			data: reclaimable.data.map(Cutoff),
			identity: self.sink_row_ttl.map(|span| Cutoff(watermark.saturating_sub(span))),
			keyspaces: reclaimable
				.keyspaces
				.into_iter()
				.map(|(keyspace, at)| (keyspace, Cutoff(at)))
				.collect(),
			mapping: reclaimable.mapping.map(Cutoff),
			mapping_cursor: self.mapping_cursor.take(),
			keyspace_cursors: std::mem::take(&mut self.keyspace_cursors),
		};

		let mut budget = self.reclaim_budget;
		let mut report = ReclaimReport::default();
		let operator = &self.operator;
		let outcome = reclaim_nodes(vec![inputs], &mut txn, &mut budget, &mut report, &mut |_, groups| {
			operator.invalidate_groups(&groups);
		})?;
		txn.flush_operator_states()?;
		self.end(txn);
		let SweepOutcome {
			cursors,
			keyspace_cursors,
		} = outcome;
		self.mapping_cursor = cursors.into_iter().next().and_then(|(_, cursor)| cursor);
		self.keyspace_cursors =
			keyspace_cursors.into_iter().next().map(|(_, cursors)| cursors).unwrap_or_default();

		Ok(reclaimed_from(&report, operator_id))
	}
}

fn reclaimed_from(report: &ReclaimReport, operator: OperatorId) -> Reclaimed {
	let Some(reclaim) = report.operator(operator) else {
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

	let ms = |cutoff: &Cutoff| cutoff.instant().to_millis();

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
		cutoffs: PhaseCutoffs {
			data: reclaim.data.as_ref().map(|phase| ms(&phase.cutoff)),
			identity: reclaim.identity.as_ref().map(|phase| ms(&phase.cutoff)),
			keyspace: reclaim.keyspaces.iter().map(|keyspace| ms(&keyspace.cutoff)).max(),
			mapping: reclaim.mapping.as_ref().map(|mapping| ms(&mapping.cutoff)),
		},
		rows: report.rows,
		backlog: report.backlog,
	}
}

fn coordinate_of(change: &Change) -> DateTime {
	change.diffs
		.iter()
		.filter_map(|diff| diff.post().or_else(|| diff.pre()))
		.flat_map(|columns| columns.time().iter().copied())
		.max()
		.unwrap_or(change.changed_at)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::flow::OperatorId, change::Change},
		state::horizon::activity_buckets,
	};
	use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

	use super::coordinate_of;
	use crate::generator;

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
		let timeless = Change::from_flow(OperatorId(1), CommitVersion(1), Vec::new(), stamped);
		assert_eq!(coordinate_of(&timeless), stamped);
	}

	#[test]
	fn a_group_falls_due_one_grid_width_after_its_span_elapses() {
		// Three unit systems meet here and none is visible at the call site: horizons are declared
		// as Durations, the grid divides nanoseconds, the chaos driver speaks milliseconds. A suite
		// that guessed wrong would never make a group due and would pass asserting nothing.
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
