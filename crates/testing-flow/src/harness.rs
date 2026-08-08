// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use reifydb_abi::operator::{capabilities::OperatorCapability, timer::TimerKind};
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	encoded::bytes::EncodedBytes,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId},
		change::{Change, Diff},
	},
	key::{
		EncodableKey, Key, kind::KeyKind, operator_group_state::OperatorGroupStateKey,
		operator_state::OperatorStateKey,
	},
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_flow::{
	operator::Operator,
	timer::Timer,
	transaction::{
		ChangeCoordinate, DeferredParams, FlowTransaction,
		substrate::{FlowSubstrate, apply_operator_state},
	},
};
use reifydb_runtime::context::{
	RuntimeContext,
	clock::{Clock, MockClock},
};
use reifydb_sdk::{config::Config, operator::OperatorLogic};
use reifydb_sub_flow::{
	execution::compaction::{OperatorCompaction, compact_operator, identity_cutoff},
	operator::{
		OperatorCell,
		apply::ApplyOperator,
		native::{NativeBridgedOperator, NativeOperatorAdapter},
		scan::series::SourceSeriesOperator,
	},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_testing_chaos::operator::{reclaim::StateFootprint, subject::Subject};
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
	mapping_cursor: Option<EncodedKey>,
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
		let substrate = FlowSubstrate {
			operators: engine.inner().operator_state(),
			..FlowSubstrate::default()
		};
		Self {
			engine,
			operator,
			clock,
			version: 1,
			pending: Pending::new(),
			substrate,
			catalog: Catalog::testing(),
			sink_row_ttl: None,
			mapping_cursor: None,
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
	pub fn with_dictionaries(mut self) -> Self {
		self.catalog = self.engine.inner().catalog().clone();
		let single = self.engine.begin_admin(IdentityId::system()).expect("begin admin").single.clone();
		let registry = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single)));
		self.substrate = FlowSubstrate::with_dictionary(registry, self.engine.inner().operator_state());
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

	pub fn footprint(&mut self) -> Result<StateFootprint> {
		let operator = self.operator.id();
		let mut txn = self.begin(DateTime::default());
		let batch = txn.state_range(operator, EncodedKeyRange::all(), None, "test::harness")?;
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

	pub fn state_items(&mut self) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
		let operator = self.operator.id();
		let mut txn = self.begin(DateTime::default());
		let batch = txn.state_range(operator, EncodedKeyRange::all(), None, "test::harness")?;
		let items = batch.items.into_iter().map(|item| (item.key, item.bytes)).collect();
		self.end(txn);
		Ok(items)
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
			at: Some(at),
			version: CommitVersion(self.version),
		});
		txn
	}

	fn end(&mut self, mut txn: FlowTransaction) {
		let pending = txn.take_pending();
		apply_operator_state(&self.substrate.operators, CommitVersion(self.version), &pending);
		let mut rest = Pending::new();
		for (key, write) in pending.iter_sorted() {
			if matches!(Key::kind(key), Some(KeyKind::OperatorState)) {
				continue;
			}
			match write {
				PendingWrite::Set(row) => rest.insert(key.clone(), row.clone()),
				PendingWrite::Remove {
					announce: true,
				} => rest.remove(key.clone()),
				PendingWrite::Remove {
					announce: false,
				} => rest.remove_silent(key.clone()),
			}
		}
		self.pending = rest;
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

	pub fn settle_timers(&mut self, watermark_ms: u64) -> Result<Vec<Change>> {
		const MAX_ROUNDS: u32 = 4_096;
		let watermark = DateTime::from_epoch_millis(watermark_ms).expect("a settle watermark is representable");
		let operator = self.operator.id();
		let wheel = self.substrate.timers.clone();
		let mut emitted = Vec::new();
		let mut rounds = 0u32;
		loop {
			let mut txn = self.begin(watermark);
			let due = wheel.take_due(operator, &mut txn, watermark, usize::MAX)?;
			if due.is_empty() {
				self.end(txn);
				return Ok(emitted);
			}
			rounds += 1;
			assert!(
				rounds <= MAX_ROUNDS,
				"timer settling did not reach quiescence within {MAX_ROUNDS} rounds; the operator \
				 keeps arming timers that are already due"
			);
			for timer in due {
				txn.set_change_coordinate(ChangeCoordinate {
					at: Some(timer.at),
					version: CommitVersion(self.version),
				});
				if let Some(change) = self.operator.on_timer(&mut txn, timer)? {
					emitted.push(change);
				}
			}
			txn.flush_operator_states()?;
			self.end(txn);
		}
	}

	pub fn state_bytes(&self) -> u64 {
		self.substrate.operators.bytes(self.operator.id())
	}

	pub fn compact(&mut self, at_ms: u64) -> Result<OperatorCompaction> {
		let watermark = DateTime::from_epoch_millis(at_ms)?;
		let identity = identity_cutoff(self.sink_row_ttl, watermark);
		let store = self.substrate.operators.clone();

		let mut txn = self.begin(watermark);
		let mut cursor = self.mapping_cursor.take();
		let compacted = compact_operator(&mut txn, &store, &self.operator, watermark, identity, &mut cursor)?;
		self.mapping_cursor = cursor;
		txn.flush_operator_states()?;
		self.end(txn);
		Ok(compacted)
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
	};
	use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

	use super::coordinate_of;
	use crate::generator;

	#[test]
	fn the_coordinate_is_the_latest_row_time() {
		// A batch is one arrival, so its position is the latest event time it carries - matching what
		// the batch path freezes as the arrival frontier. Taking the first row's time instead would make
		// a group's due-ness depend on how the driver happened to order rows inside a change.
		let early = DateTime::from_epoch_millis(1_000).unwrap();
		let late = DateTime::from_epoch_millis(9_000).unwrap();

		let change = change_at(&[early, late, early]);
		assert_eq!(coordinate_of(&change), late);

		// No row time is not the same as time zero: it means the workload declared no position, and the
		// change's own stamp is the only honest answer left.
		let stamped = DateTime::from_epoch_millis(4_242).unwrap();
		let timeless = Change::from_flow(OperatorId(1), CommitVersion(1), Vec::new(), stamped);
		assert_eq!(coordinate_of(&timeless), stamped);
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

	fn footprint(&mut self) -> Result<Option<StateFootprint>> {
		Harness::footprint(self).map(Some)
	}

	fn tick(&mut self, at_ms: u64) -> Result<Option<Change>> {
		Harness::on_timer(
			self,
			Timer {
				at: DateTime::from_epoch_millis(at_ms).unwrap(),
				kind: TimerKind::Seal,
				key: EncodedKey::new(Vec::new()),
			},
		)
	}
}
