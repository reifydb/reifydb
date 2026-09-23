// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb_value::value::{
	Value,
	constraint::{Constraint, TypeConstraint, precision::Precision, scale::Scale},
	decimal::Decimal,
	value_type::ValueType,
};

#[test]
fn a_decimal_with_too_many_places_is_rejected_even_in_exponent_form() {
	// Scale must be read from the value, never the display string, or exponent form slips past the limit.
	let constraint = TypeConstraint::with_constraint(
		ValueType::Decimal,
		Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
	);
	let tiny = Decimal::from_str("0.0000001").unwrap();

	let result = constraint.validate(&Value::Decimal(tiny.clone()));

	assert!(
		result.is_err(),
		"0.0000001 has 7 decimal places and must be rejected by decimal(10, 2); it displays as {tiny} and was accepted"
	);
}
