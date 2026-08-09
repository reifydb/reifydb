// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::BigInt;
use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::{int::Int, value_type::ValueType};

#[test]
fn test_int_storage_modes() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);

	// Inline storage covers [-2^126, 2^126), the range a 127-bit two's-complement value holds.
	let mut row1 = shape.allocate_pod();
	let small = Int::from(42i64);
	shape.set_int(&mut row1, 0, &small);
	assert_eq!(shape.get_int(&row1, 0), small);

	let mut row2 = shape.allocate_pod();
	let max_inline = Int::from((1i128 << 126) - 1);
	shape.set_int(&mut row2, 0, &max_inline);
	assert_eq!(shape.get_int(&row2, 0), max_inline);

	let mut row3 = shape.allocate_pod();
	let min_inline = Int::from(-(1i128 << 126));
	shape.set_int(&mut row3, 0, &min_inline);
	assert_eq!(shape.get_int(&row3, 0), min_inline);

	// Above the inline range the value spills into the dynamic section.
	let mut row4 = shape.allocate_pod();
	let huge =
		Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_int(&mut row4, 0, &huge);
	assert_eq!(shape.get_int(&row4, 0), huge);

	assert_eq!(shape.get_int(&row1, 0), small);
	assert_eq!(shape.get_int(&row2, 0), max_inline);
	assert_eq!(shape.get_int(&row3, 0), min_inline);
}
