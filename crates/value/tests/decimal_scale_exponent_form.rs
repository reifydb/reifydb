// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb_value::value::{
	Value,
	constraint::{Constraint, TypeConstraint, precision::Precision, scale::Scale},
	decimal::Decimal,
	value_type::ValueType,
};

fn prices() -> TypeConstraint {
	TypeConstraint::with_constraint(
		ValueType::Decimal,
		Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
	)
}

#[test]
fn a_decimal_in_exponent_form_rounds_to_the_declared_scale() {
	// Scale must come from the value, never the display string, or exponent form keeps digits the column cannot hold.
	let tiny = Decimal::from_str("0.0000001").expect("a decimal literal");
	let mut value = Value::Decimal(tiny.clone());

	prices().coerce(&mut value).expect("decimal(10, 2) must accept a value it can round");

	let Value::Decimal(stored) = &value else {
		panic!("coercion must leave the value a decimal");
	};
	let (mantissa, scale) = stored.0.as_bigint_and_exponent();
	assert_eq!(scale, 2, "0.0000001 displays as {tiny} and must land at scale 2, not keep seven places");
	assert_eq!(mantissa.to_string(), "0", "0.0000001 must round to zero at scale 2, got {stored}");
}

#[test]
fn a_decimal_is_rounded_half_away_from_zero() {
	// Banker's rounding would send 0.125 to 0.12 and disagree with every other decimal engine.
	let mut value = Value::Decimal(Decimal::from_str("0.125").expect("a decimal literal"));

	prices().coerce(&mut value).expect("decimal(10, 2) must accept a value it can round");

	assert_eq!(value.to_string(), "0.13", "0.125 must round away from zero to 0.13");
}

#[test]
fn a_decimal_wider_than_its_precision_is_rejected() {
	// Rounding must never hide an integer part the column cannot store.
	let mut value = Value::Decimal(Decimal::from_str("123456789.99").expect("a decimal literal"));

	let result = prices().coerce(&mut value);

	assert!(result.is_err(), "decimal(10, 2) holds 8 digits left of the point and must reject 123456789.99");
}
