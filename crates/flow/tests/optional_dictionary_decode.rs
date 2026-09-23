// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "runtime")]

use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	shape::{RowFamily, RowShape},
};
use reifydb_core::{
	actors::pending::Pending,
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	row::row_shape_from_columns,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::{HostOperator, host::TxnHostContext, scan::table::SourceTableOperator},
	transaction::{DeferredParams, deferred::DeferredTransaction, substrate::FlowSubstrate},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber};

fn flow_txn(engine: &TestEngine) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: Pending::new(),
		query: Some(parent.multi.begin_query().unwrap()),
		state_query: Some(parent.multi.begin_query().unwrap()),
		catalog: engine.inner().catalog().clone(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(0)),
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
	})
}

fn encode(shape: &RowShape, values: &[Value]) -> EncodedBytes {
	let mut row = shape.allocate_table();
	shape.set_values(&mut row, values);
	row.set_timestamps(DateTime::from_nanos(1), DateTime::from_nanos(1));
	row.freeze_bytes()
}

#[test]
fn a_table_scan_decodes_an_optional_dictionary_column_that_holds_an_absence() {
	// An Option-wrapped dictionary column must still be decoded, never passed on as raw entry ids.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE test");
	engine.admin("CREATE DICTIONARY test::codes FOR utf8 AS uint4");
	engine.admin("CREATE TABLE test::t { id: int4, code: Option(utf8) with { dictionary: test::codes } }");
	let catalog = engine.inner().catalog();
	let namespace = catalog.cache().find_namespace_by_name("test").expect("namespace");
	let dictionary = catalog.cache().find_dictionary_by_name(namespace.id(), "codes").expect("dictionary");
	let table = catalog.cache().find_table_by_name(namespace.id(), "t").expect("table");
	let entry =
		engine.inner().dictionary_allocators().intern(&dictionary, &Value::Utf8("aa".to_string())).unwrap().id;

	let shape = row_shape_from_columns(RowFamily::Table, &table.columns);
	let rows =
		[encode(&shape, &[Value::Int4(1), entry.to_value()]), encode(&shape, &[Value::Int4(2), Value::none()])];
	let columns = Columns::from_encoded_bytes(&shape, &[RowNumber(1), RowNumber(2)], &rows);

	let mut txn = flow_txn(&engine);
	let mut operator = SourceTableOperator::new(OperatorId(1), table);
	let change = Change::from_flow(
		OperatorId(1),
		ChangeVersion::from(CommitVersion(1)),
		vec![Diff::insert(columns)],
		DateTime::default(),
	);
	let out = operator.apply(&mut TxnHostContext::new(&mut txn, OperatorId(1)), change).unwrap();

	let Diff::Insert {
		post,
		..
	} = &out.diffs[0]
	else {
		panic!("a table scan must pass an insert through as an insert");
	};
	let code = post.column("code").expect("code column");
	assert_eq!(
		code.data().get_value(0),
		Value::Utf8("aa".to_string()),
		"the present row must decode to 'aa', got column {:?}",
		code.data()
	);
	assert!(
		matches!(code.data().get_value(1), Value::None { .. }),
		"the absent row must stay absent, got column {:?}",
		code.data()
	);
}
