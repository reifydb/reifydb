// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_codec::key::encoded::EncodedKeyRange;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
	},
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	fragment::Fragment,
	value::{
		container::number::NumberContainer, datetime::DateTime, row_number::RowNumber,
		system_columns::SystemColumns,
	},
};

use crate::{
	context::FlowContext,
	operator::{
		HostOperator,
		distinct::operator::{DistinctOperator, DistinctPlan},
		host::TxnHostContext,
		state::store,
	},
	transaction::{
		deferred::DeferredTransaction, mock::FlowTxn, row_number::RowNumberExtension, state::StateExtension,
	},
};

fn make_op(operator_id: u64, engine: &TestEngine) -> DistinctOperator {
	let routines = engine.executor().routines.clone();
	let rc = RuntimeContext::with_clock(engine.clock().clone());
	DistinctOperator::new(
		None,
		OperatorId(operator_id),
		Vec::new(),
		routines,
		rc,
		Arc::new(FlowContext::default()),
		None,
	)
}

fn host(txn: &mut DeferredTransaction, operator: OperatorId) -> TxnHostContext<'_, DeferredTransaction> {
	TxnHostContext::new(txn, operator)
}

fn build_insert(value: i64, row_num: u64) -> Change {
	let cols = vec![ColumnWithName::new(
		Fragment::internal("k"),
		ColumnBuffer::Int8(NumberContainer::from_parts(vec![value])),
	)];
	let now = DateTime::default();
	let columns = Columns::with_system(
		cols,
		SystemColumns::new(vec![RowNumber(row_num)], Vec::new(), vec![now], vec![now], vec![now]),
	);
	let mut diffs = Diffs::new();
	diffs.push(Diff::insert(columns));
	Change::from_flow(OperatorId(99), CommitVersion(1), diffs, now)
}

fn build_remove(value: i64, row_num: u64) -> Change {
	let cols = vec![ColumnWithName::new(
		Fragment::internal("k"),
		ColumnBuffer::Int8(NumberContainer::from_parts(vec![value])),
	)];
	let now = DateTime::default();
	let columns = Columns::with_system(
		cols,
		SystemColumns::new(vec![RowNumber(row_num)], Vec::new(), vec![now], vec![now], vec![now]),
	);
	let mut diffs = Diffs::new();
	diffs.push(Diff::remove(columns));
	Change::from_flow(OperatorId(99), CommitVersion(1), diffs, now)
}

fn persisted_rows(op: &DistinctOperator, txn: &mut DeferredTransaction) -> BTreeMap<Vec<u8>, Vec<u8>> {
	let mut out = BTreeMap::new();
	let batch = txn.state_range(op.plan.operator, EncodedKeyRange::all(), None, "test").unwrap();
	for item in batch.items {
		let decoded = OperatorStateKey::decode(&item.key).expect("internal state key");
		if decoded.keyspace == Keyspace::DISTINCT_ENTRY {
			out.insert(decoded.inner().as_bytes().to_vec(), item.bytes.to_vec());
		}
	}
	if let Some(row) = layout_row(op, txn) {
		out.insert(vec![u8::MAX], row);
	}
	out
}

fn layout_row(op: &DistinctOperator, txn: &mut DeferredTransaction) -> Option<Vec<u8>> {
	store::state_get(&mut host(txn, op.plan.operator), &DistinctPlan::layout_storage_key())
		.unwrap()
		.map(|row| row.body().to_vec())
}

fn entry_groups(op: &DistinctOperator, txn: &mut DeferredTransaction) -> Vec<GroupId> {
	let mut out = Vec::new();
	let batch = txn.state_range(op.plan.operator, EncodedKeyRange::all(), None, "test").unwrap();
	for item in batch.items {
		let decoded = OperatorStateKey::decode(&item.key).expect("internal state key");
		if decoded.keyspace == Keyspace::DISTINCT_ENTRY {
			out.push(decoded.group);
		}
	}
	out
}

fn erase_group_data(op: &DistinctOperator, txn: &mut DeferredTransaction, group: GroupId) -> usize {
	let batch = txn.state_range(op.plan.operator, EncodedKeyRange::all(), None, "test").unwrap();
	let mut erased = 0;
	for item in batch.items {
		let decoded = OperatorStateKey::decode(&item.key).expect("internal state key");
		if decoded.group == group && decoded.keyspace.is_data() {
			let key = GroupStateKey::from_framed(decoded.inner())
				.expect("distinct state rows carry a framed inner key");
			txn.state_remove(op.plan.operator, &key).unwrap();
			erased += 1;
		}
	}
	erased
}

#[test]
fn apply_persists_only_mutated_entries() {
	let engine = TestEngine::new();
	let mock_clock = engine.mock_clock();
	let mut op = make_op(4, &engine);
	let operator = op.plan.operator;
	let mut txn = engine.flow_txn().catalog(engine.catalog()).deferred();

	op.apply(&mut host(&mut txn, operator), build_insert(42, 1)).unwrap();
	op.apply(&mut host(&mut txn, operator), build_insert(43, 2)).unwrap();
	let after_first = persisted_rows(&op, &mut txn);
	assert_eq!(after_first.len(), 3, "two distinct entry rows plus the layout row");

	mock_clock.advance_millis(10);
	op.apply(&mut host(&mut txn, operator), build_remove(42, 99)).unwrap();
	assert_eq!(persisted_rows(&op, &mut txn), after_first, "a read-only touch must not rewrite any persisted row");

	mock_clock.advance_millis(10);
	op.apply(&mut host(&mut txn, operator), build_insert(44, 3)).unwrap();
	let after_third = persisted_rows(&op, &mut txn);
	assert_eq!(after_third.len(), 4, "exactly one new distinct entry row");
	for (key, row) in &after_first {
		assert_eq!(after_third.get(key), Some(row), "untouched rows must stay byte-identical");
	}
}

#[test]
fn a_value_whose_entry_was_reclaimed_republishes_over_the_row_the_sink_still_holds() {
	let engine = TestEngine::new();
	let mut op = make_op(6, &engine);
	let operator = op.plan.operator;
	let mut txn = engine.flow_txn().catalog(engine.catalog()).deferred();

	let first = op.apply(&mut host(&mut txn, operator), build_insert(42, 1)).unwrap();
	let Some(Diff::Insert {
		post,
		..
	}) = first.diffs.first()
	else {
		panic!("the first sighting of a value must be an insert");
	};
	let published = post.row_numbers()[0];
	let groups = entry_groups(&op, &mut txn);
	assert_eq!(groups.len(), 1, "precondition: exactly one distinct entry is persisted");
	let erased = erase_group_data(&op, &mut txn, groups[0]);
	assert!(erased > 0, "precondition: compaction must have erased the entry");
	assert!(
		txn.get_row_number(op.plan.operator, groups[0], &store::empty_key()).unwrap().is_some(),
		"precondition: the floor must leave the mapping behind, or there is nothing to collide with"
	);

	op = make_op(6, &engine);

	let second = op.apply(&mut host(&mut txn, operator), build_insert(42, 2)).unwrap();
	let Some(diff) = second.diffs.first() else {
		panic!("a value the operator has forgotten must be republished, not swallowed");
	};
	let Diff::Update {
		post,
		..
	} = diff
	else {
		panic!("republishing over a row the sink still holds must be an update, got {diff:?}");
	};
	assert_eq!(
		post.row_numbers()[0],
		published,
		"and it must reuse the row number the sink already knows, or the value now occupies two rows"
	);
}

#[test]
fn layout_row_rewritten_only_on_change() {
	let engine = TestEngine::new();
	let mock_clock = engine.mock_clock();
	let mut op = make_op(5, &engine);
	let operator = op.plan.operator;
	let mut txn = engine.flow_txn().catalog(engine.catalog()).deferred();

	op.apply(&mut host(&mut txn, operator), build_insert(42, 1)).unwrap();
	let first_layout = layout_row(&op, &mut txn).expect("layout row present after the first apply");

	mock_clock.advance_millis(10);
	op.apply(&mut host(&mut txn, operator), build_insert(45, 2)).unwrap();
	assert_eq!(layout_row(&op, &mut txn), Some(first_layout), "an unchanged layout must not be rewritten");
}
