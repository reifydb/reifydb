// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{
		constraint::{precision::Precision, scale::Scale},
		value_type::ValueType,
	},
};

#[test]
fn multiplying_two_wide_scale_decimal_columns_keeps_six_fraction_digits() {
	// Both columns store 1.5 with 39 trailing zeros, so the exact product overflowed 256 bits before any rounding.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: decimal(76, 40), b: decimal(76, 40) }");
	t.command("INSERT test::t [{ a: 1.5, b: 1.5 }]");
	let r = t.inner().query_as(TestEngine::identity(), "FROM test::t | map { v: a * b }", Params::None);
	if let Some(e) = r.error {
		panic!("query failed: {e:?}")
	}
	let column = r.frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
	assert_eq!(column.data.get_type(), ValueType::decimal(Precision::new(76), Scale::new(6)));
	assert_eq!(column.data.get_value(0).to_string(), "2.250000");
}
