// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Variable type edge case tests for the encoded encoding system

use num_bigint::BigInt;
use reifydb_core::encoded::schema::Schema;
use reifydb_type::value::{int::Int, r#type::Type};

#[test]
fn test_int_storage_modes() {
	let schema = Schema::testing(&[Type::Int]);

	// Test inline storage (fits in 127 bits)
	let mut row1 = schema.allocate();
	let small = Int::from(42i64);
	schema.set_int(&mut row1, 0, &small);
	assert_eq!(schema.get_int(&row1, 0), small);

	// Test boundary values for inline storage in separate rows
	let mut row2 = schema.allocate();
	let max_inline = Int::from((1i128 << 126) - 1);
	schema.set_int(&mut row2, 0, &max_inline);
	assert_eq!(schema.get_int(&row2, 0), max_inline);

	let mut row3 = schema.allocate();
	let min_inline = Int::from(-(1i128 << 126));
	schema.set_int(&mut row3, 0, &min_inline);
	assert_eq!(schema.get_int(&row3, 0), min_inline);

	// Test dynamic storage (exceeds 127 bits)
	let mut row4 = schema.allocate();
	let huge =
		Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999999999", 10).unwrap());
	schema.set_int(&mut row4, 0, &huge);
	assert_eq!(schema.get_int(&row4, 0), huge);

	// Verify all previous values are still intact
	assert_eq!(schema.get_int(&row1, 0), small);
	assert_eq!(schema.get_int(&row2, 0), max_inline);
	assert_eq!(schema.get_int(&row3, 0), min_inline);
}
