// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb::{Value, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::decimal::Decimal;

fn decimal(text: &str) -> Value {
	Value::Decimal(Decimal::from_str(text).unwrap())
}

fn equal_decimals_db() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace app");
	db.admin("create table app::t { id: int4, d: decimal }");
	db.command("insert app::t [{ id: 1, d: 1.5 }, { id: 2, d: 1.50 }]");
	db
}

#[test]
fn distinct_merges_decimals_that_differ_only_in_trailing_zeros() {
	// 1.5 and 1.50 are equal, so a distinct keyed on them must never keep both.
	let db = equal_decimals_db();

	let frames = db.query("from app::t | distinct { d }");

	assert_eq!(
		column_values(&frames[0], "d").len(),
		1,
		"distinct over 1.5 and 1.50 must yield one row, got {:?}",
		column_values(&frames[0], "d")
	);
}

#[test]
fn group_by_puts_decimals_that_differ_only_in_trailing_zeros_in_one_group() {
	// 1.5 and 1.50 are equal, so grouping on them must never split the count.
	let db = equal_decimals_db();

	let frames = db.query("from app::t | aggregate { n: math::count(id) } by { d }");

	assert_eq!(
		column_values(&frames[0], "n").len(),
		1,
		"group by over 1.5 and 1.50 must yield one group, got keys {:?} with counts {:?}",
		column_values(&frames[0], "d"),
		column_values(&frames[0], "n")
	);
}

#[test]
fn insert_rounds_a_decimal_whose_scale_hides_in_exponent_form() {
	// Scale is a property of the value, so exponent display must never bypass decimal(10, 2).
	let db = TestDb::memory();
	db.admin("create namespace app");
	db.admin("create table app::prices { amount: decimal(10, 2) }");

	db.command("insert app::prices [{ amount: 0.0000001 }]");

	assert_eq!(
		column_values(&db.query("from app::prices")[0], "amount"),
		vec![decimal("0.00")],
		"0.0000001 must round into decimal(10, 2) as 0.00 rather than keep seven places"
	);
}

#[test]
fn round_breaks_a_tie_away_from_zero() {
	// Banker's rounding would send 0.125 to 0.12 and disagree with the scale a decimal column rounds to.
	let db = TestDb::memory();

	let frames = db.query("from [{ v: 0.125 }, { v: 0.135 }] map { r: math::round(v, 2) }");

	assert_eq!(
		column_values(&frames[0], "r"),
		vec![decimal("0.13"), decimal("0.14")],
		"0.125 and 0.135 must both round away from zero, not to the even digit"
	);
}

#[test]
fn round_keeps_every_digit_of_a_wide_decimal() {
	// Rounding a decimal must stay exact, never pass through f64 which holds only about 16 digits.
	let db = TestDb::memory();

	let frames = db.query("from [{ v: 12345678901234567.4 }] map { r: math::round(v) }");

	assert_eq!(
		column_values(&frames[0], "r"),
		vec![decimal("12345678901234567")],
		"round(12345678901234567.4) must be exactly 12345678901234567"
	);
}
