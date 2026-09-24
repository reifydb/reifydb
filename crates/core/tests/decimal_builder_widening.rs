// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};
use reifydb_value::value::{
	Value,
	constraint::{precision::Precision, scale::Scale},
	decimal::Decimal,
	value_type::ValueType,
};

fn decimal(text: &str) -> Value {
	Value::Decimal(Decimal::parse(text).unwrap())
}

fn build(ty: ValueType, values: &[&str]) -> ColumnBuffer {
	let mut builder = ColumnBuilder::with_capacity(ty, values.len());
	for value in values {
		builder.push_value(decimal(value));
	}
	builder.finish()
}

fn texts(buffer: &ColumnBuffer) -> Vec<String> {
	(0..buffer.len()).map(|i| buffer.get_value(i).to_string()).collect()
}

#[test]
fn a_finer_scale_widens_a_column_that_declares_every_whole_digit() {
	// decimal(76, 1) reserves 75 whole digits, so sizing from the declaration leaves no room for a second fraction digit.
	let buffer = build(ValueType::decimal(Precision::new(76), Scale::new(1)), &["1.5", "1.25"]);
	assert_eq!(buffer.get_type(), ValueType::decimal(Precision::new(76), Scale::new(2)));
	assert_eq!(texts(&buffer), vec!["1.50", "1.25"]);
}

#[test]
fn widening_keeps_the_declared_whole_digits_when_they_fit() {
	// Shrinking to the digits in use would narrow a column that still had room.
	let buffer = build(ValueType::decimal(Precision::new(10), Scale::new(1)), &["1.5", "1.25"]);
	assert_eq!(buffer.get_type(), ValueType::decimal(Precision::new(11), Scale::new(2)));
	assert_eq!(texts(&buffer), vec!["1.50", "1.25"]);
}

#[test]
fn values_that_need_more_than_76_digits_together_round_the_finer_one_half_up() {
	// 70 whole digits plus 8 fraction digits is 78, so the fraction must round to 6 digits instead of panicking.
	let big = format!("1{}", "0".repeat(69));
	let buffer = build(ValueType::decimal(Precision::new(76), Scale::new(0)), &[&big, "0.00000051"]);
	assert_eq!(buffer.get_type(), ValueType::decimal(Precision::new(76), Scale::new(6)));
	assert_eq!(texts(&buffer), vec![format!("{big}.000000"), "0.000001".to_string()]);
}

#[test]
fn rounding_the_finer_value_carries_into_its_whole_digit() {
	// Half up on 0.9999999 must carry to 1.000000, a truncating rescale would give 0.999999.
	let big = format!("1{}", "0".repeat(69));
	let buffer = build(ValueType::decimal(Precision::new(76), Scale::new(0)), &[&big, "0.9999999"]);
	assert_eq!(texts(&buffer), vec![format!("{big}.000000"), "1.000000".to_string()]);
}
