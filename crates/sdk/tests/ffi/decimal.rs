// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::str::FromStr;

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::decimal::Decimal;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn decimal_zero() {
	let input = ColumnBuffer::decimal([Decimal::zero()]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("decimal_zero", &input, &output);
}

#[test]
fn decimal_small_values() {
	let input = ColumnBuffer::decimal([
		Decimal::from_i64(1),
		Decimal::from_i64(-1),
		Decimal::from_str("3.14159265358979323846").expect("parse"),
		Decimal::from_str("-2.71828182845904523536").expect("parse"),
	]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("decimal_small", &input, &output);
}

#[test]
fn decimal_high_precision() {
	let input = ColumnBuffer::decimal([
		Decimal::from_str("0.0000000000000000000000000000001").expect("parse"),
		Decimal::from_str("99999999999999999999999999999999").expect("parse"),
	]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("decimal_high_precision", &input, &output);
}

#[test]
fn decimal_thirty_two_rows() {
	let values: Vec<Decimal> = (0..32i64).map(Decimal::from_i64).collect();
	let input = ColumnBuffer::decimal(values);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("decimal_thirty_two", &input, &output);
}

#[test]
fn decimal_with_undefined() {
	let input = ColumnBuffer::decimal_optional([
		Some(Decimal::from_i64(42)),
		None,
		Some(Decimal::zero()),
		None,
		Some(Decimal::one()),
	]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("decimal_with_undefined", &input, &output);
}
