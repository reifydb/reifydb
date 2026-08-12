// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, OperatorStateKey},
	},
	metrics::heap::OperatorSample,
	state::store::TimerKind,
};
use reifydb_flow::{operator::Operator, timer::Timer, transaction::FlowTransaction};
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	flow::operator::{OperatorLogic, timer::Timer as SdkTimer, view::bridge::BridgeChangeView},
};
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		duration::Duration,
		row_number::RowNumber,
	},
};
use tracing::error;

use crate::operator::context::bridge::{Bridge, BridgeOperatorContext};

fn run_or_abort<R>(operator: OperatorId, stage: &'static str, f: impl FnOnce() -> SdkResult<R>) -> R {
	match catch_unwind(AssertUnwindSafe(f)) {
		Ok(Ok(value)) => value,
		Ok(Err(e)) => {
			error!(
				operator_id = operator.0,
				stage,
				"bridged operator returned an error; operators must not fail - aborting: {:?}",
				e
			);
			abort();
		}
		Err(_) => {
			error!(operator_id = operator.0, stage, "bridged operator panicked - aborting");
			abort();
		}
	}
}

fn unscope(key: &EncodedKey) -> Option<GroupStateKey> {
	GroupStateKey::from_framed(OperatorStateKey::decode(key)?.inner())
}

pub trait BridgedOperator: Send {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &'static [OperatorCapability];

	fn apply(&mut self, bridge: &mut dyn Bridge, change: Change) -> Result<Change>;

	fn on_timer(&mut self, _bridge: &mut dyn Bridge, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn seal_after(&self) -> Option<Duration> {
		None
	}

	fn flush_state(&mut self, _bridge: &mut dyn Bridge) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub type BoxedBridgedOperator = Box<dyn BridgedOperator>;

pub struct FlowBridge<'a, T: FlowTransaction> {
	txn: &'a mut T,
	operator: OperatorId,
	now: DateTime,
}

impl<'a, T: FlowTransaction> FlowBridge<'a, T> {
	pub fn new(txn: &'a mut T, operator: OperatorId) -> Self {
		let now = txn.written_at();
		Self {
			txn,
			operator,
			now,
		}
	}
}

impl<T: FlowTransaction> Bridge for FlowBridge<'_, T> {
	fn written_at(&self) -> DateTime {
		self.now
	}
	fn version(&self) -> CommitVersion {
		self.txn.version()
	}
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedBytes>> {
		Ok(self.txn.state_get(self.operator, key)?.map(EncodedOperatorRow::into_bytes))
	}
	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedBytes)>> {
		Ok(self.txn
			.state_get_many(self.operator, keys)?
			.items
			.into_iter()
			.filter_map(|r| Some((unscope(&r.key)?, r.bytes)))
			.collect())
	}
	fn state_set(&mut self, key: &GroupStateKey, row: EncodedBytes) -> Result<()> {
		self.txn.state_set(self.operator, key, EncodedOperatorRow::try_from(row)?)
	}
	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.txn.state_remove(self.operator, key)
	}
	fn state_clear(&mut self) -> Result<()> {
		self.txn.state_clear(self.operator)
	}
	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedBytes)>> {
		Ok(self.txn
			.state_range_all(self.operator, range)?
			.items
			.into_iter()
			.filter_map(|r| Some((unscope(&r.key)?, r.bytes)))
			.collect())
	}
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>> {
		Ok(self.txn.intern_groups(self.operator, groups)?.into_iter().map(|(group, _)| group).collect())
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		groups.iter().map(|group| self.txn.lookup_group(self.operator, group)).collect()
	}
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.arm_timer(
			self.operator,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}
	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer(
			self.operator,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(self.txn.flow_watermark())
	}
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.operator, group, keys)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.operator, group, key).map(|_| ())
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		self.txn.remove_row_numbers_below(self.operator, group, upper)
	}
	fn dictionary_id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>> {
		Ok(self.txn.find_dictionary_by_name(name).map(|d| d.id))
	}
	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.find_in_dictionary(&dict, value),
			None => Ok(None),
		}
	}
	fn dictionary_get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.get_from_dictionary(&dict, id),
			None => Ok(None),
		}
	}
	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(&GroupStateKey, &EncodedBytes) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch = self.txn.state_get_many(self.operator, keys).map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			let Some(key) = unscope(&r.key) else {
				continue;
			};
			visit(&key, &r.bytes)?;
		}
		Ok(())
	}
}

pub struct BridgeOperatorAdapter<C> {
	logic: C,
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
}

impl<C> BridgeOperatorAdapter<C> {
	pub fn new(logic: C, operator: OperatorId, capabilities: &'static [OperatorCapability]) -> Self {
		Self {
			logic,
			operator,
			capabilities,
		}
	}
}

impl<C: OperatorLogic + 'static> BridgedOperator for BridgeOperatorAdapter<C> {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &'static [OperatorCapability] {
		self.capabilities
	}

	fn apply(&mut self, bridge: &mut dyn Bridge, change: Change) -> Result<Change> {
		let version = change.version;
		let changed_at = change.changed_at;
		let mut ctx = BridgeOperatorContext::new(bridge, self.operator);
		{
			let view = BridgeChangeView::new(&change);
			let logic = &mut self.logic;
			run_or_abort(self.operator, "apply", || logic.apply(&mut ctx, view));
		}
		let diffs = ctx.take_diffs();
		Ok(Change::from_flow(self.operator, version, diffs, changed_at))
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.logic.sample()
	}

	fn seal_after(&self) -> Option<Duration> {
		self.logic.seal_after()
	}

	fn on_timer(&mut self, bridge: &mut dyn Bridge, timer: Timer) -> Result<Option<Change>> {
		let at = timer.at;
		let version = bridge.version();
		let mut ctx = BridgeOperatorContext::new(bridge, self.operator);
		{
			let logic = &mut self.logic;
			run_or_abort(self.operator, "on_timer", || {
				logic.on_timer(
					&mut ctx,
					SdkTimer {
						at,
						kind: timer.kind,
						key: timer.key.as_ref(),
					},
				)
			});
		}
		let diffs = ctx.take_diffs();
		if diffs.is_empty() {
			return Ok(None);
		}
		Ok(Some(Change::from_flow(self.operator, version, diffs, at)))
	}

	fn flush_state(&mut self, bridge: &mut dyn Bridge) -> Result<()> {
		let mut ctx = BridgeOperatorContext::new(bridge, self.operator);
		let logic = &mut self.logic;
		run_or_abort(self.operator, "flush_state", || logic.flush_state(&mut ctx));
		Ok(())
	}
}

pub struct BridgeOperator {
	inner: BoxedBridgedOperator,
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
}

impl BridgeOperator {
	pub fn new(
		inner: BoxedBridgedOperator,
		operator: OperatorId,
		capabilities: &'static [OperatorCapability],
	) -> Self {
		Self {
			inner,
			operator,
			capabilities,
		}
	}
}

impl<T: FlowTransaction> Operator<T> for BridgeOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.capabilities
	}

	fn apply(&mut self, txn: &mut T, change: Change) -> Result<Change> {
		let mut bridge = FlowBridge::new(txn, self.operator);
		self.inner.apply(&mut bridge, change)
	}

	fn flush(&mut self, txn: &mut T) -> Result<()> {
		let mut bridge = FlowBridge::new(txn, self.operator);
		self.inner.flush_state(&mut bridge)
	}

	fn seal_span(&self) -> Option<Duration> {
		self.inner.seal_after().filter(|span| !span.is_zero())
	}

	fn on_timer(&mut self, txn: &mut T, timer: Timer) -> Result<Option<Change>> {
		let mut bridge = FlowBridge::new(txn, self.operator);
		self.inner.on_timer(&mut bridge, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.inner.sample()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::change::Change,
		key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	};
	use reifydb_flow::{
		operator::Operator,
		transaction::{ChangeCoordinate, FlowTransaction, deferred::DeferredTransaction},
	};
	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::{
		Bridge, BridgeOperator, BridgedOperator, EncodedKey, EncodedKeyRange, EncodedOperatorRow, FlowBridge,
		GroupStateKey, OperatorCapability, OperatorId,
	};

	const NODE: OperatorId = OperatorId(1);

	fn key(name: &str) -> EncodedKey {
		EncodedKey::new(name.as_bytes())
	}

	#[test]
	fn a_dylib_read_resolves_a_group_without_creating_one() {
		// A read must never intern, or groups already reclaimed resurrect and the dictionary never shrinks.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
			version: CommitVersion(7),
		});
		let mut bridge = FlowBridge::new(&mut txn, NODE);

		assert_eq!(bridge.lookup_groups(&[key("absent")]).unwrap(), vec![None]);

		let interned = bridge.intern_groups(&[key("absent")]).unwrap();
		assert_eq!(
			interned,
			vec![GroupId::FIRST],
			"the earlier read must not have consumed an id from the counter"
		);
	}

	fn stored_key(suffix: &str) -> GroupStateKey {
		OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::ACCUMULATOR, suffix.as_bytes())
	}

	#[test]
	fn a_dylib_batch_read_hands_back_the_key_the_guest_wrote() {
		// The store returns operator-scoped keys, so handing one back unstripped makes the guest's own lookups miss.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
			version: CommitVersion(7),
		});
		let mut bridge = FlowBridge::new(&mut txn, NODE);

		let written = stored_key("entry");
		bridge.state_set(&written, EncodedOperatorRow::timeless(&[7]).into()).unwrap();

		let from_get_many: Vec<GroupStateKey> =
			bridge.state_get_many(&[written.clone()]).unwrap().into_iter().map(|(key, _)| key).collect();
		assert_eq!(from_get_many, vec![written.clone()], "state_get_many must return the key that was written");

		let from_range: Vec<GroupStateKey> = bridge
			.state_range(EncodedKeyRange::all())
			.unwrap()
			.into_iter()
			.map(|(key, _)| key)
			.collect();
		assert_eq!(from_range, vec![written.clone()], "state_range must return the key that was written");

		let mut visited = Vec::new();
		bridge.state_get_many_visit(&[written.clone()], &mut |key, _| {
			visited.push(key.clone());
			Ok(())
		})
		.unwrap();
		assert_eq!(visited, vec![written], "state_get_many_visit must visit the key that was written");
	}

	struct RecordingBridged;

	impl BridgedOperator for RecordingBridged {
		fn id(&self) -> OperatorId {
			NODE
		}

		fn capabilities(&self) -> &'static [OperatorCapability] {
			&[]
		}

		fn apply(&mut self, _bridge: &mut dyn Bridge, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn seal_after(&self) -> Option<Duration> {
			Some(Duration::from_milliseconds_const(65_000))
		}
	}

	#[test]
	fn the_host_wrapper_forwards_the_seal_span_to_the_frontier_walk() {
		// A wrapper that swallows the seal span claims a frontier covering buckets still amendable.
		let wrapper = BridgeOperator::new(Box::new(RecordingBridged), NODE, &[]);

		assert_eq!(
			Operator::<DeferredTransaction>::seal_span(&wrapper),
			Some(Duration::from_milliseconds(65_000).unwrap())
		);
	}
}
