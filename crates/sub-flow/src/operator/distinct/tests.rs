// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_codec::key::encoded::EncodedKeyRange;
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
	key::{
		EncodableKey,
		flow_node_state::FlowNodeStateKey,
		operator_state::{Keyspace, OperatorStateKey},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_runtime::context::RuntimeContext;
use reifydb_test_harness::operator::transaction::FlowTxn;
use reifydb_value::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{container::number::NumberContainer, datetime::DateTime, row_number::RowNumber},
};

use crate::{
	context::FlowContext,
	operator::{
		OperatorCell, Operators, distinct::operator::DistinctOperator, scan::view::PrimitiveViewOperator,
		stateful::utils,
	},
};

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

fn make_op(node_id: u64, engine: &TestEngine) -> DistinctOperator {
	let routines = engine.executor().routines.clone();
	let rc = RuntimeContext::with_clock(engine.clock().clone());
	DistinctOperator::new(
		noop_parent(),
		FlowNodeId(node_id),
		Vec::new(),
		routines,
		rc,
		Arc::new(FlowContext::default()),
	)
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

fn persisted_rows(op: &DistinctOperator, txn: &mut FlowTransaction) -> BTreeMap<Vec<u8>, Vec<u8>> {
	let mut out = BTreeMap::new();
	let batch = txn.state_range(op.id(), EncodedKeyRange::all(), None).unwrap();
	for item in batch.items {
		let inner = FlowNodeStateKey::decode(&item.key).expect("internal state key");
		if let Some((_, keyspace, _)) = OperatorStateKey::decode_inner(&inner.key) {
			if keyspace == Keyspace::DISTINCT_ENTRY {
				out.insert(inner.key.clone(), item.row.to_vec());
			}
		}
	}
	if let Some(row) = layout_row(op, txn) {
		out.insert(vec![u8::MAX], row);
	}
	out
}

fn layout_row(op: &DistinctOperator, txn: &mut FlowTransaction) -> Option<Vec<u8>> {
	utils::state_scan_all(op.id(), txn).unwrap().into_iter().next().map(|(_, row)| row.to_vec())
}

#[test]
fn flush_persists_only_mutated_entries() {
	let engine = TestEngine::new();
	let mock_clock = engine.mock_clock();
	let op = make_op(4, &engine);
	let mut txn = engine.flow_txn().catalog(engine.catalog()).deferred();

	op.apply(&mut txn, build_insert(42, 1)).unwrap();
	op.apply(&mut txn, build_insert(43, 2)).unwrap();
	txn.flush_operator_states().unwrap();
	let after_first = persisted_rows(&op, &mut txn);
	assert_eq!(after_first.len(), 3, "two distinct entry rows plus the layout row");

	mock_clock.advance_millis(10);
	op.apply(&mut txn, build_remove(42, 99)).unwrap();
	txn.flush_operator_states().unwrap();
	assert_eq!(persisted_rows(&op, &mut txn), after_first, "a read-only touch must not rewrite any persisted row");

	mock_clock.advance_millis(10);
	op.apply(&mut txn, build_insert(44, 3)).unwrap();
	txn.flush_operator_states().unwrap();
	let after_third = persisted_rows(&op, &mut txn);
	assert_eq!(after_third.len(), 4, "exactly one new distinct entry row");
	for (key, row) in &after_first {
		assert_eq!(after_third.get(key), Some(row), "untouched rows must stay byte-identical");
	}
}

#[test]
fn layout_row_rewritten_only_on_change() {
	let engine = TestEngine::new();
	let mock_clock = engine.mock_clock();
	let op = make_op(5, &engine);
	let mut txn = engine.flow_txn().catalog(engine.catalog()).deferred();

	op.apply(&mut txn, build_insert(42, 1)).unwrap();
	txn.flush_operator_states().unwrap();
	let first_layout = layout_row(&op, &mut txn).expect("layout row present after the first flush");

	mock_clock.advance_millis(10);
	op.apply(&mut txn, build_insert(45, 2)).unwrap();
	txn.flush_operator_states().unwrap();
	assert_eq!(layout_row(&op, &mut txn), Some(first_layout), "an unchanged layout must not be rewritten");
}
