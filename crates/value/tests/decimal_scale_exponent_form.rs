// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use arrow_buffer::i256;
use reifydb_value::value::{
	Value,
	constraint::{TypeConstraint, precision::Precision, scale::Scale},
	decimal::Decimal,
	value_type::ValueType,
};

fn prices() -> TypeConstraint {
	TypeConstraint::unconstrained(ValueType::decimal(Precision::new(10), Scale::new(2)))
}

#[test]
fn a_decimal_in_exponent_form_is_refused_on_write_and_rounds_on_cast() {
	// Scale comes from the value, not the display text, or exponent form keeps digits the column cannot hold.
	let tiny = Decimal::from_str("0.0000001").expect("a decimal literal");
	let mut value = Value::Decimal(tiny.clone());

	let error = prices().coerce(&mut value).expect_err("decimal(10, 2) must refuse to round 0.0000001 on write");
	assert_eq!(error.code, "CONSTRAINT_006");

	let stored = tiny.round_to_scale(2).expect("a cast to scale 2 always fits 0.0000001");
	assert_eq!(stored.scale(), 2, "0.0000001 displays as {tiny} and must land at scale 2, not keep seven places");
	assert_eq!(stored.unscaled(), i256::ZERO, "0.0000001 must round to zero at scale 2, got {stored}");
}

#[test]
fn a_decimal_is_rounded_half_away_from_zero() {
	// Banker's rounding would send 0.125 to 0.12 and disagree with every other decimal engine.
	let decimal = Decimal::from_str("0.125").expect("a decimal literal");
	let mut value = Value::Decimal(decimal.clone());

	let error = prices().coerce(&mut value).expect_err("decimal(10, 2) must refuse to round 0.125 on write");
	assert_eq!(error.code, "CONSTRAINT_006");

	let cast = decimal.round_to_scale(2).expect("a cast to scale 2 always fits 0.125");
	assert_eq!(cast.to_string(), "0.13", "0.125 must round away from zero to 0.13");
	let negative = Decimal::from_str("-0.125").expect("a decimal literal").round_to_scale(2).unwrap();
	assert_eq!(negative.to_string(), "-0.13", "-0.125 must round away from zero to -0.13");
}

#[test]
fn a_decimal_wider_than_its_precision_is_rejected() {
	// Rounding must never hide an integer part the column cannot store.
	let mut value = Value::Decimal(Decimal::from_str("123456789.99").expect("a decimal literal"));

	let error = prices().coerce(&mut value).expect_err("decimal(10, 2) holds 8 digits left of the point");

	assert_eq!(error.code, "CONSTRAINT_005", "123456789.99 must be refused for its precision, not its scale");
}

#[test]
fn a_zero_decimal_keeps_the_scale_it_was_rounded_to() {
	// Dropping the fraction on zero hides the declared scale and disagrees with what every non-zero value renders.
	let zero = Decimal::from_str("0.0000001").expect("a decimal literal").round_to_scale(2).unwrap();
	let mut one = Value::Decimal(Decimal::from_str("1.5").expect("a decimal literal"));

	prices().coerce(&mut one).expect("decimal(10, 2) must accept 1.5, which needs no rounding");

	assert_eq!(one.to_string(), "1.50", "a non-zero value pads to the declared scale");
	assert_eq!(zero.to_string(), "0.00", "so zero must pad too, not collapse to 0");
}
