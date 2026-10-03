// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::identifier::ColumnObject,
	value::{batch::batch as scalar_batch, column::factory},
};
use reifydb_evaluate::stack::{SymbolTable, Variable};

use crate::common::{Env, access, batch, column, frag, rows, strings};

#[test]
fn a_duplicate_column_name_reads_the_first_match() {
	// Reading a later duplicate would silently filter on the wrong column after a join or extend.
	let env = Env::new();
	let input = batch(vec![factory::int4("a", [1, 2]), factory::int4("a", [10, 20])]);

	let lowered = env.lowered(&column("a"), input.clone()).unwrap();
	let old = env.old(&column("a"), input).unwrap();

	assert_eq!(strings(&lowered), vec!["1", "2"]);
	assert_eq!(lowered, old);
}

#[test]
fn a_system_column_named_without_its_hash_reads_the_system_column() {
	// Without the system column rule, rownum would resolve to a user column or not resolve at all.
	let env = Env::new();
	let input = batch(vec![factory::uint8("#rownum", [7, 8]), factory::int4("a", [1, 2])]);

	let lowered = env.lowered(&column("rownum"), input.clone()).unwrap();
	let old = env.old(&column("rownum"), input).unwrap();

	assert_eq!(strings(&lowered), vec!["7", "8"]);
	assert_eq!(lowered.0.name(), "rownum");
	assert_eq!(lowered, old);
}

#[test]
fn a_column_name_that_names_a_scalar_frame_variable_reads_its_row_zero() {
	// A one row, one column frame must act as a value, repeated for every row of the batch.
	let mut symbols = SymbolTable::new();
	let frame = scalar_batch(vec![factory::int4("x", [7])]).unwrap();
	symbols.set(
		"x".to_string(),
		Variable::Columns {
			batch: frame.clone(),
		},
		false,
	)
	.unwrap();
	let env = Env::with_symbols(symbols);

	let lowered = env.lowered(&column("x"), rows(3)).unwrap();

	assert_eq!(strings(&lowered), vec!["7", "7", "7"]);
	assert_eq!(lowered.0, frame.schema().fields()[0]);
}

#[test]
fn a_column_name_that_names_a_frame_of_two_rows_is_not_found() {
	// A frame with more than one row is no value, so it must not stand in for a missing column.
	let mut symbols = SymbolTable::new();
	symbols.set(
		"x".to_string(),
		Variable::Columns {
			batch: scalar_batch(vec![factory::int4("x", [7, 8])]).unwrap(),
		},
		false,
	)
	.unwrap();
	let env = Env::with_symbols(symbols);

	let lowered = env.lowered(&column("x"), rows(3)).unwrap_err();
	let old = env.old(&column("x"), rows(3)).unwrap_err();

	assert_eq!(lowered.code, "QUERY_001");
	assert_eq!(lowered.code, old.code);
	assert_eq!(lowered.fragment.text(), old.fragment.text());
}

#[test]
fn a_qualified_access_reads_the_dotted_name_or_the_bare_name() {
	// The bare name rule is what lets t.a find a column the scan named plain a.
	let env = Env::new();
	let qualified = || ColumnObject::Qualified {
		namespace: frag("ns"),
		name: frag("t"),
	};

	let dotted = batch(vec![factory::int4("t.a", [1]), factory::int4("a", [2])]);
	assert_eq!(strings(&env.lowered(&access(qualified(), "a"), dotted).unwrap()), vec!["1"]);

	let bare = batch(vec![factory::int4("b", [5]), factory::int4("a", [6])]);
	let lowered = env.lowered(&access(qualified(), "a"), bare.clone()).unwrap();
	assert_eq!(strings(&lowered), vec!["6"]);
	assert_eq!(lowered, env.old(&access(qualified(), "a"), bare).unwrap());
}

#[test]
fn a_qualified_access_skips_system_columns_but_keeps_their_position() {
	// The index must count the system columns it skips, or the lowered column points one slot early.
	let env = Env::new();
	let input = batch(vec![factory::uint8("#rownum", [9]), factory::int4("t.a", [3])]);
	let expr = access(ColumnObject::Alias(frag("t")), "a");

	let lowered = env.lowered(&expr, input.clone()).unwrap();

	assert_eq!(strings(&lowered), vec!["3"]);
	assert_eq!(lowered, env.old(&expr, input).unwrap());
}

#[test]
fn a_missed_access_reports_column_not_found_with_the_dotted_text() {
	// The error must name t.a, not a, or the user cannot tell which source was searched.
	let env = Env::new();
	let expr = access(ColumnObject::Alias(frag("t")), "a");

	let lowered = env.lowered(&expr, batch(vec![factory::int4("a", [1])])).unwrap_err();
	let old = env.old(&expr, batch(vec![factory::int4("a", [1])])).unwrap_err();

	assert_eq!(lowered.code, "QUERY_001");
	assert_eq!(lowered.fragment.text(), "t.a");
	assert_eq!(lowered.code, old.code);
	assert_eq!(lowered.fragment.text(), old.fragment.text());
}
