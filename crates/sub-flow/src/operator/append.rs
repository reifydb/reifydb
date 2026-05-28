// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, ops::Bound, time::Duration};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		shape::RowShape,
	},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	internal,
	row::TtlAnchor,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::columns::Columns,
};
use reifydb_sdk::operator::Tick;
use reifydb_type::{Result, error::Error, value::row_number::RowNumber};

use crate::{
	operator::{
		Operator, OperatorCell,
		stateful::{
			row::RowNumberProvider,
			utils::{internal_state_drop, internal_state_get, internal_state_range, internal_state_set},
		},
	},
	transaction::FlowTransaction,
};

const TIMESTAMP_PREFIX: u8 = b'T';

pub struct AppendOperator {
	node: FlowNodeId,

	parents: Vec<OperatorCell>,

	input_nodes: Vec<FlowNodeId>,

	row_number_provider: RowNumberProvider,

	ttl_nanos: Option<u64>,

	ttl_anchor: TtlAnchor,

	evict_cursor: RefCell<Option<EncodedKey>>,
}

impl AppendOperator {
	pub fn new(
		node: FlowNodeId,
		parents: Vec<OperatorCell>,
		input_nodes: Vec<FlowNodeId>,
		ttl_nanos: Option<u64>,
		ttl_anchor: TtlAnchor,
	) -> Self {
		debug_assert_eq!(parents.len(), input_nodes.len());
		debug_assert!(parents.len() >= 2, "Append requires at least 2 inputs");

		Self {
			node,
			parents,
			input_nodes,
			row_number_provider: RowNumberProvider::new(node),
			ttl_nanos,
			ttl_anchor,
			evict_cursor: RefCell::new(None),
		}
	}

	#[cfg(test)]
	pub(crate) fn new_for_state_tests(node: FlowNodeId, ttl_nanos: Option<u64>, ttl_anchor: TtlAnchor) -> Self {
		Self {
			node,
			parents: Vec::new(),
			input_nodes: Vec::new(),
			row_number_provider: RowNumberProvider::new(node),
			ttl_nanos,
			ttl_anchor,
			evict_cursor: RefCell::new(None),
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parents[0].output_schema()
	}

	fn parent_index_for_origin(&self, origin: &ChangeOrigin) -> Option<usize> {
		match origin {
			ChangeOrigin::Flow(from_node) => self.input_nodes.iter().position(|n| n == from_node),
			ChangeOrigin::Shape(_) => None,
		}
	}

	fn make_composite_key(parent_index: u8, source_row: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(parent_index);
		serializer.extend_u64(source_row.0);
		serializer.finish()
	}

	fn make_timestamp_key(composite_key: &EncodedKey) -> EncodedKey {
		let mut bytes = Vec::with_capacity(1 + composite_key.len());
		bytes.push(TIMESTAMP_PREFIX);
		bytes.extend_from_slice(composite_key.as_ref());
		EncodedKey::new(bytes)
	}

	fn touch_timestamp(&self, txn: &mut FlowTransaction, composite_key: &EncodedKey) -> Result<()> {
		if self.ttl_nanos.is_none() {
			return Ok(());
		}
		let key = Self::make_timestamp_key(composite_key);
		let now_nanos = txn.clock().now_nanos();
		let shape = RowShape::operator_state();
		let (mut row, created_at) = match internal_state_get(self.node, txn, &key)? {
			Some(existing) => {
				let c = existing.created_at_nanos();
				(
					existing,
					if c == 0 {
						now_nanos
					} else {
						c
					},
				)
			}
			None => (shape.allocate(), now_nanos),
		};
		row.set_timestamps(created_at, now_nanos);
		internal_state_set(self.node, txn, &key, row)
	}

	fn forget_mapping(&self, txn: &mut FlowTransaction, composite_key: &EncodedKey) -> Result<()> {
		self.row_number_provider.remove_for_key(txn, composite_key)?;
		let ts_key = Self::make_timestamp_key(composite_key);
		internal_state_drop(self.node, txn, &ts_key)
	}
}

impl Operator for AppendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		if self.ttl_nanos.is_some() {
			Some(Duration::from_secs(1))
		} else {
			None
		}
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let parent_origin = change.origin.clone();
		let mut result_diffs = Vec::with_capacity(change.diffs.len());

		for diff in change.diffs {
			let diff_origin = diff.origin().cloned().unwrap_or_else(|| parent_origin.clone());
			let parent_index = self.parent_index_for_origin(&diff_origin).ok_or_else(|| {
				Error(Box::new(internal!("Append received diff from unknown node: {:?}", diff_origin)))
			})?;
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					if let Some(d) = self.translate_append_insert(txn, parent_index, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					if let Some(d) = self.translate_append_update(txn, parent_index, pre, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					if let Some(d) = self.translate_append_remove(txn, parent_index, pre)? {
						result_diffs.push(d);
					}
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result_diffs, change.changed_at))
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let Some(ttl_nanos) = self.ttl_nanos else {
			return Ok(None);
		};

		let now_nanos = tick.now.to_nanos();
		let cutoff = now_nanos.saturating_sub(ttl_nanos);

		const EVICT_BATCH: usize = 4096;
		let prefix = [TIMESTAMP_PREFIX];
		let base = EncodedKeyRange::prefix(&prefix);
		let start = match self.evict_cursor.borrow().clone() {
			Some(cursor) => Bound::Excluded(cursor),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch =
			internal_state_range(self.node, txn, range).take(EVICT_BATCH).collect::<Result<Vec<_>>>()?;
		let reached_end = batch.len() < EVICT_BATCH;
		let last_key = batch.last().map(|(key, _)| key.clone());

		for (storage_key, row) in batch {
			let anchor_ts = match self.ttl_anchor {
				TtlAnchor::Created => row.created_at_nanos(),
				TtlAnchor::Updated => row.updated_at_nanos(),
			};
			if anchor_ts >= cutoff {
				continue;
			}

			let bytes = storage_key.as_ref();
			if bytes.is_empty() || bytes[0] != TIMESTAMP_PREFIX {
				continue;
			}
			let composite_key = EncodedKey::new(bytes[1..].to_vec());
			self.forget_mapping(txn, &composite_key)?;
		}

		*self.evict_cursor.borrow_mut() = if reached_end {
			None
		} else {
			last_key
		};
		Ok(None)
	}
}

impl AppendOperator {
	#[inline]
	fn translate_create_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		source: &Columns,
	) -> Result<Vec<RowNumber>> {
		let row_count = source.row_count();
		let mut output_row_numbers = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let source_row_number = source.row_numbers[row_idx];
			let composite_key = Self::make_composite_key(parent_index as u8, source_row_number);
			let (output_row_number, _) =
				self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;
			self.touch_timestamp(txn, &composite_key)?;
			output_row_numbers.push(output_row_number);
		}
		Ok(output_row_numbers)
	}

	#[inline]
	fn lookup_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		source: &Columns,
	) -> Result<Option<(Vec<RowNumber>, Vec<EncodedKey>)>> {
		let row_count = source.row_count();
		let mut output_row_numbers = Vec::with_capacity(row_count);
		let mut composite_keys = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let source_row_number = source.row_numbers[row_idx];
			let composite_key = Self::make_composite_key(parent_index as u8, source_row_number);
			let Some(row_number) = self.row_number_provider.get_row_number(txn, &composite_key)? else {
				return Ok(None);
			};
			output_row_numbers.push(row_number);
			composite_keys.push(composite_key);
		}
		Ok(Some((output_row_numbers, composite_keys)))
	}

	#[inline]
	fn translate_append_insert(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let output_row_numbers = self.translate_create_row_numbers(txn, parent_index, &post)?;
		let output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::insert(output)))
	}

	#[inline]
	fn translate_append_update(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		pre: Columns,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let Some((output_row_numbers, composite_keys)) = self.lookup_row_numbers(txn, parent_index, &pre)?
		else {
			return Ok(None);
		};
		for composite_key in &composite_keys {
			self.touch_timestamp(txn, composite_key)?;
		}
		let pre_output = pre.with_row_numbers(output_row_numbers.clone());
		let post_output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::update(pre_output, post_output)))
	}

	#[inline]
	fn translate_append_remove(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		pre: Columns,
	) -> Result<Option<Diff>> {
		if pre.row_count() == 0 {
			return Ok(None);
		}
		let Some((output_row_numbers, composite_keys)) = self.lookup_row_numbers(txn, parent_index, &pre)?
		else {
			return Ok(None);
		};
		for composite_key in &composite_keys {
			self.forget_mapping(txn, composite_key)?;
		}
		let output = pre.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::remove(output)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::common::CommitVersion;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::Clock;
	use reifydb_sdk::operator::Tick;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::value::{datetime::DateTime, identity::IdentityId};

	use super::*;
	use crate::operator::stateful::utils::internal_state_get;

	fn make_tick(clock: &Clock) -> Tick {
		Tick {
			now: DateTime::from_nanos(clock.now_nanos()),
		}
	}

	fn composite(parent: u8, source_row: u64) -> EncodedKey {
		AppendOperator::make_composite_key(parent, RowNumber(source_row))
	}

	#[test]
	fn translate_create_assigns_and_persists_mapping() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let op = AppendOperator::new_for_state_tests(FlowNodeId(1), None, TtlAnchor::Created);

		let key = composite(0, 42);
		assert_eq!(op.row_number_provider.get_row_number(&mut txn, &key).unwrap(), None);

		let (assigned, was_new) = op.row_number_provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(was_new);
		assert_eq!(op.row_number_provider.get_row_number(&mut txn, &key).unwrap(), Some(assigned));
	}

	#[test]
	fn forget_mapping_removes_forward_and_timestamp_entries() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let op = AppendOperator::new_for_state_tests(FlowNodeId(2), Some(1_000), TtlAnchor::Created);

		let key = composite(1, 7);
		let (_assigned, _) = op.row_number_provider.get_or_create_row_number(&mut txn, &key).unwrap();
		op.touch_timestamp(&mut txn, &key).unwrap();

		assert!(op.row_number_provider.get_row_number(&mut txn, &key).unwrap().is_some());
		let ts_key = AppendOperator::make_timestamp_key(&key);
		assert!(internal_state_get(op.node, &mut txn, &ts_key).unwrap().is_some());

		op.forget_mapping(&mut txn, &key).unwrap();

		assert!(op.row_number_provider.get_row_number(&mut txn, &key).unwrap().is_none());
		assert!(internal_state_get(op.node, &mut txn, &ts_key).unwrap().is_none());
	}

	#[test]
	fn touch_timestamp_is_noop_when_ttl_disabled() {
		// without ttl we must not waste storage on timestamp entries, since they would never
		// be consulted
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let op = AppendOperator::new_for_state_tests(FlowNodeId(3), None, TtlAnchor::Created);

		let key = composite(0, 5);
		op.touch_timestamp(&mut txn, &key).unwrap();

		let ts_key = AppendOperator::make_timestamp_key(&key);
		assert!(
			internal_state_get(op.node, &mut txn, &ts_key).unwrap().is_none(),
			"timestamp must not be written when ttl is disabled"
		);
	}

	#[test]
	fn touch_timestamp_preserves_created_at_across_calls() {
		// the created anchor is meaningful only if created_at stays pinned across re-touches
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let op = AppendOperator::new_for_state_tests(FlowNodeId(4), Some(60_000_000_000), TtlAnchor::Created);

		let key = composite(0, 1);
		op.touch_timestamp(&mut txn, &key).unwrap();
		let ts_key = AppendOperator::make_timestamp_key(&key);
		let first = internal_state_get(op.node, &mut txn, &ts_key).unwrap().unwrap();
		let created_at = first.created_at_nanos();
		assert_ne!(created_at, 0);

		mock_clock.advance_millis(100);
		op.touch_timestamp(&mut txn, &key).unwrap();
		let second = internal_state_get(op.node, &mut txn, &ts_key).unwrap().unwrap();
		assert_eq!(
			second.created_at_nanos(),
			created_at,
			"created_at must not move when ttl is anchored to Created"
		);
		assert!(second.updated_at_nanos() > created_at, "updated_at must advance with the clock");
	}

	#[test]
	fn tick_evicts_mappings_older_than_ttl_with_created_anchor() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let ttl_nanos = 50_000_000; // 50ms
		let op = AppendOperator::new_for_state_tests(FlowNodeId(5), Some(ttl_nanos), TtlAnchor::Created);

		let old = composite(0, 100);
		op.row_number_provider.get_or_create_row_number(&mut txn, &old).unwrap();
		op.touch_timestamp(&mut txn, &old).unwrap();

		mock_clock.advance_millis(40);
		let young = composite(0, 200);
		op.row_number_provider.get_or_create_row_number(&mut txn, &young).unwrap();
		op.touch_timestamp(&mut txn, &young).unwrap();

		mock_clock.advance_millis(20);
		let result = op.tick(&mut txn, make_tick(&engine.clock())).unwrap();
		assert!(result.is_none(), "append tick never produces a downstream change");

		assert!(
			op.row_number_provider.get_row_number(&mut txn, &old).unwrap().is_none(),
			"old mapping must be evicted past the TTL"
		);
		assert!(
			op.row_number_provider.get_row_number(&mut txn, &young).unwrap().is_some(),
			"young mapping must survive when its timestamp is within the TTL window"
		);
	}

	#[test]
	fn tick_with_updated_anchor_keeps_recently_touched_mappings_alive() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let ttl_nanos = 50_000_000; // 50ms
		let op = AppendOperator::new_for_state_tests(FlowNodeId(6), Some(ttl_nanos), TtlAnchor::Updated);

		let key = composite(0, 1);
		op.row_number_provider.get_or_create_row_number(&mut txn, &key).unwrap();
		op.touch_timestamp(&mut txn, &key).unwrap();

		mock_clock.advance_millis(40);
		op.touch_timestamp(&mut txn, &key).unwrap();

		mock_clock.advance_millis(40);
		op.tick(&mut txn, make_tick(&engine.clock())).unwrap();

		assert!(
			op.row_number_provider.get_row_number(&mut txn, &key).unwrap().is_some(),
			"Updated anchor must keep mapping alive while it is being touched within the TTL window"
		);

		mock_clock.advance_millis(100);
		op.tick(&mut txn, make_tick(&engine.clock())).unwrap();
		assert!(
			op.row_number_provider.get_row_number(&mut txn, &key).unwrap().is_none(),
			"after silence longer than TTL the mapping must finally be evicted"
		);
	}

	#[test]
	fn tick_is_noop_when_ttl_disabled() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let op = AppendOperator::new_for_state_tests(FlowNodeId(7), None, TtlAnchor::Created);

		let key = composite(0, 1);
		op.row_number_provider.get_or_create_row_number(&mut txn, &key).unwrap();

		let result = op.tick(&mut txn, make_tick(&engine.clock())).unwrap();
		assert!(result.is_none());
		assert!(op.row_number_provider.get_row_number(&mut txn, &key).unwrap().is_some());
	}

	#[test]
	fn capabilities_always_include_tick() {
		// Mirrors join/distinct: the operator always declares the Tick capability so the
		// engine can route per-flow ticks (set via `with { tick: ... }` on the view) here
		// even when TTL is disabled. Tick is a no-op in that case, but the capability is
		// required to avoid the engine's enforce_tick_capability abort.
		let with_ttl = AppendOperator::new_for_state_tests(FlowNodeId(8), Some(100), TtlAnchor::Created);
		assert!(with_ttl.capabilities().contains(&OperatorCapability::Tick));
		let without_ttl = AppendOperator::new_for_state_tests(FlowNodeId(9), None, TtlAnchor::Created);
		assert!(without_ttl.capabilities().contains(&OperatorCapability::Tick));
	}
}
