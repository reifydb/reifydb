// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::state::OperatorState;
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
			let bytes = DistinctOperator::state_bytes(row, "DistinctEntry")?;
			if bytes.body().is_empty() {
				continue;
			}
			let archived = DistinctEntry::archived(&bytes).map_err(|e| {
				Error::from(FlowStateError::Decode {
					state: "DistinctEntry",
					cause: e.to_string(),
				})
			})?;
			if archived.last_seen_nanos.to_native() < cutoff {
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
	use std::{collections::BTreeMap, sync::Arc};

	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::{
				flow::FlowNodeId,
				id::{NamespaceId, TableId, ViewId},
				view::{TableView, View, ViewKind},
			},
			change::{Change, Diff, Diffs},
		},
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_sdk::operator::Tick;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{
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
		operator::{Operator, OperatorCell, Operators, scan::view::PrimitiveViewOperator},
		transaction::FlowTransaction,
	};

	// The distinct operator only consults its parent for output_schema, so
	// any cheap cell works; a columnless source view is the smallest real
	// operator now that the raw Custom container is gone.
	fn noop_parent() -> OperatorCell {
		let view = View::Table(TableView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "noop".to_string(),
			kind: ViewKind::Deferred,
			columns: vec![],
			primary_key: None,
			underlying: TableId(1),
			sort: vec![],
		});
		OperatorCell::new(Operators::SourceView(PrimitiveViewOperator::new(FlowNodeId(0), view)))
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

	fn build_remove(value: i64, row_num: u64) -> Change {
		let cols = vec![ColumnWithName::new(
			Fragment::internal("k"),
			ColumnBuffer::Int8(NumberContainer::from_parts(CowVec::new(vec![value]))),
		)];
		let now = DateTime::default();
		let columns = Columns::with_system_columns(cols, vec![RowNumber(row_num)], vec![now], vec![now]);
		let mut diffs = Diffs::new();
		diffs.push(Diff::remove(columns));
		Change::from_flow(FlowNodeId(99), CommitVersion(1), diffs, now)
	}

	fn state_rows(op: &DistinctOperator, txn: &mut FlowTransaction) -> BTreeMap<Vec<u8>, Vec<u8>> {
		utils::state_scan_all(op.id(), txn)
			.unwrap()
			.into_iter()
			.map(|(k, row)| (k.as_ref().to_vec(), row.to_vec()))
			.collect()
	}

	fn make_op(node_id: u64, ttl_nanos: Option<u64>, engine: &TestEngine) -> DistinctOperator {
		let routines = engine.executor().routines.clone();
		let rc = RuntimeContext::with_clock(engine.clock().clone());
		DistinctOperator::new(
			noop_parent(),
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

	#[test]
	fn flush_skips_clean_entries() {
		// A flush must rewrite only entries mutated in the slice. Touching an
		// entry read-only (a remove whose row number is not present) must leave
		// its persisted row and the layout row byte-identical; the clock advance
		// makes any rewrite visible through the row timestamps.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(4, None, &engine);
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
		txn.flush_operator_states().unwrap();
		let after_first = state_rows(&op, &mut txn);
		assert_eq!(after_first.len(), 3, "two entries plus the layout row");

		mock_clock.advance_millis(10);
		op.apply(&mut txn, build_remove(42, 99)).unwrap();
		txn.flush_operator_states().unwrap();
		assert_eq!(
			state_rows(&op, &mut txn),
			after_first,
			"a read-only touch must not rewrite any persisted row"
		);

		mock_clock.advance_millis(10);
		op.apply(&mut txn, build_insert(44, 3)).unwrap();
		txn.flush_operator_states().unwrap();
		let after_third = state_rows(&op, &mut txn);
		assert_eq!(after_third.len(), 4, "exactly one new entry row");
		for (key, row) in &after_first {
			assert_eq!(after_third.get(key), Some(row), "untouched rows must stay byte-identical");
		}
	}

	#[test]
	fn layout_row_rewritten_only_on_change() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(5, None, &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			engine.catalog(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		op.apply(&mut txn, build_insert(42, 1)).unwrap();
		txn.flush_operator_states().unwrap();
		let layout_key = utils::state_scan_all(op.id(), &mut txn)
			.unwrap()
			.into_iter()
			.map(|(k, _)| k)
			.find(|k| DistinctOperator::hash_from_entry_key(k.as_ref()).is_none())
			.expect("layout row present after the first flush");
		let first_layout = state_rows(&op, &mut txn).remove(layout_key.as_ref()).unwrap();

		mock_clock.advance_millis(10);
		op.apply(&mut txn, build_insert(45, 2)).unwrap();
		txn.flush_operator_states().unwrap();
		assert_eq!(
			state_rows(&op, &mut txn).remove(layout_key.as_ref()),
			Some(first_layout),
			"an unchanged layout must not be rewritten"
		);
	}
}
