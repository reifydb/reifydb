// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	cell::{Cell, UnsafeCell},
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
	key::operator_state::{GroupId, GroupStateKey},
	metrics::heap::OperatorSample,
	state::store::TimerKind,
};
use reifydb_flow::{
	operator::Operator,
	timer::Timer,
	transaction::{DepFlowTransaction, slot::PersistFn},
};
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

pub trait BridgedOperator: Send {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &'static [OperatorCapability];

	fn apply(&self, bridge: &mut dyn Bridge, change: Change) -> Result<Change>;

	fn on_timer(&self, _bridge: &mut dyn Bridge, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn seal_after(&self) -> Option<Duration> {
		None
	}

	fn flush_state(&self, _bridge: &mut dyn Bridge) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub type BoxedBridgedOperator = Box<dyn BridgedOperator>;

pub struct FlowBridge<'a> {
	txn: &'a mut DepFlowTransaction,
	operator: OperatorId,
	now: DateTime,
}

impl<'a> FlowBridge<'a> {
	pub fn new(txn: &'a mut DepFlowTransaction, operator: OperatorId) -> Self {
		let now = txn.written_at();
		Self {
			txn,
			operator,
			now,
		}
	}
}

impl Bridge for FlowBridge<'_> {
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
			.filter_map(|r| GroupStateKey::from_framed(r.key).map(|k| (k, r.bytes)))
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
			.filter_map(|r| GroupStateKey::from_framed(r.key).map(|k| (k, r.bytes)))
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
			let Some(key) = GroupStateKey::from_framed(r.key.clone()) else {
				continue;
			};
			visit(&key, &r.bytes)?;
		}
		Ok(())
	}
}

pub struct BridgeOperatorAdapter<C> {
	logic: UnsafeCell<C>,
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
}

impl<C> BridgeOperatorAdapter<C> {
	pub fn new(logic: C, operator: OperatorId, capabilities: &'static [OperatorCapability]) -> Self {
		Self {
			logic: UnsafeCell::new(logic),
			operator,
			capabilities,
		}
	}
}

unsafe impl<C: Send> Send for BridgeOperatorAdapter<C> {}

impl<C: OperatorLogic + 'static> BridgedOperator for BridgeOperatorAdapter<C> {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &'static [OperatorCapability] {
		self.capabilities
	}

	fn apply(&self, bridge: &mut dyn Bridge, change: Change) -> Result<Change> {
		let version = change.version;
		let changed_at = change.changed_at;
		let mut ctx = BridgeOperatorContext::new(bridge, self.operator);
		{
			let view = BridgeChangeView::new(&change);
			// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time, and the
			// logic only reaches the context, never back into this cell; no other borrow is live.
			let logic = unsafe { &mut *self.logic.get() };
			run_or_abort(self.operator, "apply", || logic.apply(&mut ctx, view));
		}
		let diffs = ctx.take_diffs();
		Ok(Change::from_flow(self.operator, version, diffs, changed_at))
	}

	fn sample(&self) -> Option<OperatorSample> {
		// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time and no apply or
		// timer call is in flight here; no other borrow of the cell is live.
		let logic = unsafe { &*self.logic.get() };
		logic.sample()
	}

	fn seal_after(&self) -> Option<Duration> {
		// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time and no apply or
		// timer call is in flight here; no other borrow of the cell is live.
		let logic = unsafe { &*self.logic.get() };
		logic.seal_after()
	}

	fn on_timer(&self, bridge: &mut dyn Bridge, timer: Timer) -> Result<Option<Change>> {
		let at = timer.at;
		let version = bridge.version();
		let mut ctx = BridgeOperatorContext::new(bridge, self.operator);
		{
			// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time, and the
			// logic only reaches the context, never back into this cell; no other borrow is live.
			let logic = unsafe { &mut *self.logic.get() };
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

	fn flush_state(&self, bridge: &mut dyn Bridge) -> Result<()> {
		let mut ctx = BridgeOperatorContext::new(bridge, self.operator);
		// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time, and the logic
		// only reaches the context, never back into this cell; no other borrow is live.
		let logic = unsafe { &mut *self.logic.get() };
		run_or_abort(self.operator, "flush_state", || logic.flush_state(&mut ctx));
		Ok(())
	}
}

#[derive(Clone, Copy)]
struct SendableBridged(*const dyn BridgedOperator);
unsafe impl Send for SendableBridged {}

pub struct BridgeOperator {
	inner: BoxedBridgedOperator,
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
	last_registered_txn: Cell<u64>,
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
			last_registered_txn: Cell::new(u64::MAX),
		}
	}

	fn ensure_flush_slot(&self, txn: &mut DepFlowTransaction) -> Result<()> {
		let txn_version = txn.version().0;
		if self.last_registered_txn.get() != txn_version {
			let captured = SendableBridged(&*self.inner as *const dyn BridgedOperator);
			let operator = self.operator;
			let persist: PersistFn = Box::new(move |txn: &mut DepFlowTransaction, _value: Box<dyn Any>| {
				let captured = captured;
				// SAFETY: captured.0 points at the heap allocation of self.inner, which is stable
				// across moves of the wrapper and outlives the transaction running this persist
				// closure, since the actor owning the operator also drives that transaction.
				let bridged = unsafe { &*captured.0 };
				let mut bridge = FlowBridge::new(txn, operator);
				bridged.flush_state(&mut bridge)?;
				Ok(())
			});
			let _ = txn.operator_state::<(), _>(operator, move |_txn| Ok(((), persist)))?;
			txn.mark_state_dirty(operator);
			self.last_registered_txn.set(txn_version);
		}
		Ok(())
	}
}

unsafe impl Send for BridgeOperator {}

impl Operator<DepFlowTransaction> for BridgeOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.capabilities
	}

	fn apply(&self, txn: &mut DepFlowTransaction, change: Change) -> Result<Change> {
		self.ensure_flush_slot(txn)?;
		let mut bridge = FlowBridge::new(txn, self.operator);
		self.inner.apply(&mut bridge, change)
	}

	fn seal_span(&self) -> Option<Duration> {
		self.inner.seal_after().filter(|span| !span.is_zero())
	}

	fn on_timer(&self, txn: &mut DepFlowTransaction, timer: Timer) -> Result<Option<Change>> {
		self.ensure_flush_slot(txn)?;
		let mut bridge = FlowBridge::new(txn, self.operator);
		self.inner.on_timer(&mut bridge, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.inner.sample()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, interface::change::Change, key::operator_state::GroupId};
	use reifydb_flow::{operator::Operator, transaction::ChangeCoordinate};
	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::{Bridge, BridgeOperator, BridgedOperator, EncodedKey, FlowBridge, OperatorCapability, OperatorId};

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

	struct RecordingBridged;

	impl BridgedOperator for RecordingBridged {
		fn id(&self) -> OperatorId {
			NODE
		}

		fn capabilities(&self) -> &'static [OperatorCapability] {
			&[]
		}

		fn apply(&self, _bridge: &mut dyn Bridge, change: Change) -> Result<Change> {
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

		assert_eq!(Operator::seal_span(&wrapper), Some(Duration::from_milliseconds(65_000).unwrap()));
	}
}
