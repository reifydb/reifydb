// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Variable type edge case tests for the row encoding system

use num_bigint::BigInt;
use reifydb_core::value::row::EncodedRowLayout;
use reifydb_type::*;

#[test]
fn test_int_storage_modes() {
	let layout = EncodedRowLayout::new(&[Type::Int]);

	// Test inline storage (fits in 127 bits)
	let mut row1 = layout.allocate_row();
	let small = Int::from(42i64);
	layout.set_int(&mut row1, 0, &small);
	assert_eq!(layout.get_int(&row1, 0), small);

	// Test boundary values for inline storage in separate rows
	let mut row2 = layout.allocate_row();
	let max_inline = Int::from((1i128 << 126) - 1);
	layout.set_int(&mut row2, 0, &max_inline);
	assert_eq!(layout.get_int(&row2, 0), max_inline);

	let mut row3 = layout.allocate_row();
	let min_inline = Int::from(-(1i128 << 126));
	layout.set_int(&mut row3, 0, &min_inline);
	assert_eq!(layout.get_int(&row3, 0), min_inline);

	// Test dynamic storage (exceeds 127 bits)
	let mut row4 = layout.allocate_row();
	let huge =
		Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999999999", 10).unwrap());
	layout.set_int(&mut row4, 0, &huge);
	assert_eq!(layout.get_int(&row4, 0), huge);

	// Verify all previous values are still intact
	assert_eq!(layout.get_int(&row1, 0), small);
	assert_eq!(layout.get_int(&row2, 0), max_inline);
	assert_eq!(layout.get_int(&row3, 0), min_inline);
}
