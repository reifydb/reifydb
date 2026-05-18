// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::shape::RowShape;
use reifydb_type::value::r#type::Type;

#[test]
fn test_boolean_bit_patterns() {
	let shape = RowShape::testing(&[Type::Boolean]);
	let mut row = shape.allocate();

	// Test transaction values
	shape.set_bool(&mut row, 0, true);
	assert_eq!(shape.get_bool(&row, 0), true);

	shape.set_bool(&mut row, 0, false);
	assert_eq!(shape.get_bool(&row, 0), false);

	// Test that undefined is different from false
	shape.set_none(&mut row, 0);
	assert!(shape.try_get_bool(&row, 0).is_none());
}

#[test]
fn test_boolean_field_independence() {
	// Test that boolean fields don't interfere with each other
	let shape = RowShape::testing(&[
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
	]);
	let mut row = shape.allocate();

	// Set alternating pattern
	for i in 0..8 {
		shape.set_bool(&mut row, i, i % 2 == 0);
	}

	// Verify pattern
	for i in 0..8 {
		assert_eq!(shape.get_bool(&row, i), i % 2 == 0);
	}

	// Change some values
	shape.set_bool(&mut row, 2, true);
	shape.set_bool(&mut row, 5, false);

	// Verify only targeted fields changed
	assert_eq!(shape.get_bool(&row, 0), true);
	assert_eq!(shape.get_bool(&row, 1), false);
	assert_eq!(shape.get_bool(&row, 2), true); // Changed
	assert_eq!(shape.get_bool(&row, 3), false);
	assert_eq!(shape.get_bool(&row, 4), true);
	assert_eq!(shape.get_bool(&row, 5), false); // Changed
	assert_eq!(shape.get_bool(&row, 6), true);
	assert_eq!(shape.get_bool(&row, 7), false);
}
