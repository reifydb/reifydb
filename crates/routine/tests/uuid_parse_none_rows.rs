// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::function::uuid::{v4::UuidV4, v7::UuidV7};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId},
};

const V4: &str = "0b6c2f8e-4c8a-4a38-9d6c-2f8e4c8a4a38";

const V7: &str = "01890a5d-ac96-774b-bcce-b302099a8057";

fn ctx(name: &str) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count: 2,
		runtime_context: &RUNTIME,
	}
}

fn parse_with_a_none_row(
	routine: &dyn Routine<FunctionContext<'static>>,
	name: &str,
	text: &str,
) -> Result<Vec<String>, RoutineError> {
	let input = ColumnBuffer::utf8_with_bitvec([text.to_string(), String::new()], vec![true, false]);
	let result = routine.call(&mut ctx(name), &Columns::new(vec![ColumnWithName::new("s", input)]))?;
	let data = result.data_at(0);
	Ok((0..data.len())
		.map(|i| match data.get_value(i) {
			Value::None {
				..
			} => "none".to_string(),
			value => value.to_string(),
		})
		.collect())
}

fn rql_values(t: &TestEngine, rql: &str) -> Result<Vec<String>, String> {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		return Err(format!("{:?}", err.diagnostic()));
	}
	let column = result.frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
	Ok((0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect())
}

#[test]
fn parsing_an_optional_text_column_gives_none_for_the_none_row() {
	// A none row holds an empty placeholder string, so parsing it must yield none, never an invalid UUID error.
	let v4 = parse_with_a_none_row(&UuidV4::new(), "uuid::v4", V4);
	let v7 = parse_with_a_none_row(&UuidV7::new(), "uuid::v7", V7);

	assert_eq!(
		(v4.map_err(|e| format!("{e:?}")), v7.map_err(|e| format!("{e:?}"))),
		(Ok(vec![V4.to_string(), "none".to_string()]), Ok(vec![V7.to_string(), "none".to_string()]))
	);
}

#[test]
fn rql_parsing_an_optional_text_column_gives_none_for_the_none_row() {
	// The query path hands the function the placeholder behind a none row, so the parse must skip it.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: Option(utf8), b: Option(utf8) }");
	t.command(&format!("INSERT test::t [{{ id: 1, a: '{V4}', b: '{V7}' }}, {{ id: 2, a: none, b: none }}]"));

	let v4 = rql_values(&t, "FROM test::t | sort { id: ASC } | map { v: uuid::v4(a) }");
	let v7 = rql_values(&t, "FROM test::t | sort { id: ASC } | map { v: uuid::v7(b) }");

	assert_eq!(
		(v4, v7),
		(Ok(vec![V4.to_string(), "none".to_string()]), Ok(vec![V7.to_string(), "none".to_string()]))
	);
}
