// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_core::interface::change::Change;
use reifydb_sdk::operator::Tick;
use reifydb_value::{Result, error::Error, util::hash::Hash128, value::duration::Duration};

use crate::{
	error::FlowStateError,
	operator::{
		distinct::{operator::DistinctOperator, state::DistinctEntry},
		stateful::utils,
	},
	transaction::FlowTransaction,
};

impl DistinctOperator {
	pub(super) fn ticks_interval(&self) -> Option<Duration> {
		if self.ttl_nanos.is_some() {
			Some(Duration::from_seconds(1).unwrap())
		} else {
			None
		}
	}

	pub(super) fn tick_evict(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let Some(ttl_nanos) = self.ttl_nanos else {
			return Ok(None);
		};
		let cutoff = tick.now.to_nanos().saturating_sub(ttl_nanos);

		let mut expired: Vec<Hash128> = Vec::new();
		for (key, row) in utils::state_scan_all(self.node, txn)? {
			let Some(hash) = Self::hash_from_entry_key(key.as_ref()) else {
				continue;
			};
			let blob = self.shape.get_blob(&row, 0);
			if blob.is_empty() {
				continue;
			}
			let entry: DistinctEntry = from_bytes(blob.as_ref()).map_err(|e| {
				Error::from(FlowStateError::Decode {
					state: "DistinctEntry",
					cause: e.to_string(),
				})
			})?;
			if entry.last_seen_nanos < cutoff {
				expired.push(hash);
			}
		}

		for hash in expired {
			utils::state_drop(self.node, txn, &Self::entry_key(hash))?;
		}

		Ok(None)
	}
}

#[cfg(test)]
mod ttl_tests {
	use std::sync::Arc;

	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::flow::FlowNodeId,
			change::{Change, Diff, Diffs},
		},
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_sdk::operator::Tick;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{
		Result,
		fragment::Fragment,
		util::cowvec::CowVec,
		value::{
			container::number::NumberContainer, datetime::DateTime, identity::IdentityId,
			row_number::RowNumber,
		},
	};

	use super::*;
	use crate::{
		context::FlowContext,
		operator::{Operator, OperatorCell, Operators},
		transaction::FlowTransaction,
	};

	struct NoOpParent;

	impl Operator for NoOpParent {
		fn id(&self) -> FlowNodeId {
			FlowNodeId(0)
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&self, _: &mut FlowTransaction, change: Change) -> Result<Change> {
			Ok(change)
		}
	}

	fn build_insert(value: i64, row_num: u64) -> Change {
		let cols = vec![ColumnWithName::new(
			Fragment::internal("k"),
			ColumnBuffer::Int8(NumberContainer::from_parts(CowVec::new(vec![value]))),
		)];
		let now = DateTime::default();
		let columns = Columns::with_system_columns(cols, vec![RowNumber(row_num)], vec![now], vec![now]);
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns));
		Change::from_flow(FlowNodeId(99), CommitVersion(1), diffs, now)
	}

	fn make_op(node_id: u64, ttl_nanos: Option<u64>, engine: &TestEngine) -> DistinctOperator {
		let routines = engine.executor().routines.clone();
		let rc = RuntimeContext::with_clock(engine.clock().clone());
		let parent: OperatorCell = OperatorCell::new(Operators::Custom(Box::new(NoOpParent)));
		DistinctOperator::new(
			parent,
			FlowNodeId(node_id),
			Vec::new(),
			routines,
			rc,
			ttl_nanos,
			Arc::new(FlowContext::default()),
		)
	}

	#[test]
	fn tick_is_noop_when_retention_is_unset() {
		let engine = TestEngine::new();
		let op = make_op(1, None, &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			engine.catalog(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		op.apply(&mut txn, build_insert(42, 1)).unwrap();
		op.apply(&mut txn, build_insert(43, 2)).unwrap();

		let result = op
			.tick(
				&mut txn,
				Tick {
					now: DateTime::from_nanos(u64::MAX),
				},
			)
			.unwrap();
		assert!(result.is_none(), "tick must return Ok(None) (silent)");

		txn.flush_operator_states().unwrap();
		assert_eq!(op.count_entries(&mut txn), 2, "no eviction when ttl is None");
	}

	#[test]
	fn tick_evicts_only_entries_past_cutoff() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		// 10ms row
		let op = make_op(2, Some(10_000_000), &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			engine.catalog(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		// Insert two entries at t = 1000ms
		op.apply(&mut txn, build_insert(42, 1)).unwrap();
		op.apply(&mut txn, build_insert(43, 2)).unwrap();

		// Advance to t = 1005ms (5ms < 10ms row) - tick must NOT evict
		mock_clock.advance_millis(5);
		let result = op
			.tick(
				&mut txn,
				Tick {
					now: DateTime::from_nanos(mock_clock.now_nanos()),
				},
			)
			.unwrap();
		assert!(result.is_none());
		txn.flush_operator_states().unwrap();
		assert_eq!(op.count_entries(&mut txn), 2);

		// Advance to t = 1020ms (20ms > 10ms row) - tick must evict both
		mock_clock.advance_millis(15);
		let result = op
			.tick(
				&mut txn,
				Tick {
					now: DateTime::from_nanos(mock_clock.now_nanos()),
				},
			)
			.unwrap();
		assert!(result.is_none(), "eviction is silent (Drop mode)");
		txn.flush_operator_states().unwrap();
		assert_eq!(op.count_entries(&mut txn), 0);
	}

	#[test]
	fn tick_keeps_recently_touched_entries() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(3, Some(10_000_000), &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			engine.catalog(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		// Insert k=42 at t = 1000ms
		op.apply(&mut txn, build_insert(42, 1)).unwrap();

		// Advance to t = 1015ms, re-insert k=42 (refreshes last_seen_nanos)
		mock_clock.advance_millis(15);
		op.apply(&mut txn, build_insert(42, 99)).unwrap();

		// Insert k=43 at t = 1015ms (this and k=42 are both fresh)
		op.apply(&mut txn, build_insert(43, 2)).unwrap();

		// Tick at t = 1020ms (5ms since both were last touched - within row)
		mock_clock.advance_millis(5);
		op.tick(
			&mut txn,
			Tick {
				now: DateTime::from_nanos(mock_clock.now_nanos()),
			},
		)
		.unwrap();
		txn.flush_operator_states().unwrap();
		assert_eq!(op.count_entries(&mut txn), 2);
	}
}
