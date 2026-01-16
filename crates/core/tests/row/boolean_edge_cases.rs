// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Boolean edge case tests for the encoded encoding system

use reifydb_core::value::encoded::layout::EncodedValuesLayout;
use reifydb_type::value::r#type::Type;

#[test]
fn test_boolean_bit_patterns() {
	let layout = EncodedValuesLayout::new(&[Type::Boolean]);
	let mut row = layout.allocate();

	// Test standard values
	layout.set_bool(&mut row, 0, true);
	assert_eq!(layout.get_bool(&row, 0), true);

	layout.set_bool(&mut row, 0, false);
	assert_eq!(layout.get_bool(&row, 0), false);

	// Test that undefined is different from false
	layout.set_undefined(&mut row, 0);
	assert!(layout.try_get_bool(&row, 0).is_none());
}

#[test]
fn test_boolean_field_independence() {
	// Test that boolean fields don't interfere with each other
	let layout = EncodedValuesLayout::new(&[
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
		Type::Boolean,
	]);
	let mut row = layout.allocate();

	// Set alternating pattern
	for i in 0..8 {
		layout.set_bool(&mut row, i, i % 2 == 0);
	}

	// Verify pattern
	for i in 0..8 {
		assert_eq!(layout.get_bool(&row, i), i % 2 == 0);
	}

	// Change some values
	layout.set_bool(&mut row, 2, true);
	layout.set_bool(&mut row, 5, false);

	// Verify only targeted fields changed
	assert_eq!(layout.get_bool(&row, 0), true);
	assert_eq!(layout.get_bool(&row, 1), false);
	assert_eq!(layout.get_bool(&row, 2), true); // Changed
	assert_eq!(layout.get_bool(&row, 3), false);
	assert_eq!(layout.get_bool(&row, 4), true);
	assert_eq!(layout.get_bool(&row, 5), false); // Changed
	assert_eq!(layout.get_bool(&row, 6), true);
	assert_eq!(layout.get_bool(&row, 7), false);
}
