// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_boolean_bit_patterns() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);
	assert_eq!(shape.get::<bool>(&row, 0), true);

	shape.set::<bool>(&mut row, 0, false);
	assert_eq!(shape.get::<bool>(&row, 0), false);

	// none must be distinguishable from false, not collapse onto it.
	shape.set_none(&mut row, 0);
	assert!(shape.try_get::<bool>(&row, 0).is_none());
}

#[test]
fn test_boolean_field_independence() {
	// Booleans pack into shared bytes, so a write to one field must not disturb its neighbours.
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[
			ValueType::Boolean,
			ValueType::Boolean,
			ValueType::Boolean,
			ValueType::Boolean,
			ValueType::Boolean,
			ValueType::Boolean,
			ValueType::Boolean,
			ValueType::Boolean,
		],
	);
	let mut row = shape.allocate_pod();

	for i in 0..8 {
		shape.set::<bool>(&mut row, i, i % 2 == 0);
	}

	for i in 0..8 {
		assert_eq!(shape.get::<bool>(&row, i), i % 2 == 0);
	}

	shape.set::<bool>(&mut row, 2, true);
	shape.set::<bool>(&mut row, 5, false);

	assert_eq!(shape.get::<bool>(&row, 0), true);
	assert_eq!(shape.get::<bool>(&row, 1), false);
	assert_eq!(shape.get::<bool>(&row, 2), true); // Changed
	assert_eq!(shape.get::<bool>(&row, 3), false);
	assert_eq!(shape.get::<bool>(&row, 4), true);
	assert_eq!(shape.get::<bool>(&row, 5), false); // Changed
	assert_eq!(shape.get::<bool>(&row, 6), true);
	assert_eq!(shape.get::<bool>(&row, 7), false);
}
