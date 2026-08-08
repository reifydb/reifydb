// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::BigInt;
use num_traits::Zero;
use reifydb_codec::row::shape::RowShape;
use reifydb_value::value::{uint::Uint, value_type::ValueType};

#[test]
fn test_u64_inline() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	let small = Uint::from(42u64);
	shape.set_uint(&mut row, 0, &small);
	assert!(row.is_defined(0));

	let retrieved = shape.get_uint(&row, 0);
	assert_eq!(retrieved, small);

	let mut row2 = shape.allocate();
	let large = Uint::from(999999999999u64);
	shape.set_uint(&mut row2, 0, &large);
	assert_eq!(shape.get_uint(&row2, 0), large);
}

#[test]
fn test_u128_boundary() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	// The top bit of the slot is the mode flag, so 2^127 - 1 is the largest value that can
	// still be stored inline.
	let large = Uint::from(u64::MAX);
	shape.set_uint(&mut row, 0, &large);
	assert!(row.is_defined(0));

	let retrieved = shape.get_uint(&row, 0);
	assert_eq!(retrieved, large);

	let mut row2 = shape.allocate();
	let max_u127 = Uint::from(u128::MAX >> 1); // 127 bits
	shape.set_uint(&mut row2, 0, &max_u127);
	assert_eq!(shape.get_uint(&row2, 0), max_u127);
}

#[test]
fn test_dynamic_storage() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	// Past 2^127, so the value is stored as little-endian magnitude bytes in the dynamic
	// section instead of the fixed slot.
	let huge = Uint::from(
		BigInt::parse_bytes(b"123456789012345678901234567890123456789012345678901234567890", 10).unwrap(),
	);

	shape.set_uint(&mut row, 0, &huge);
	assert!(row.is_defined(0));

	let retrieved = shape.get_uint(&row, 0);
	assert_eq!(retrieved, huge);
}

#[test]
fn test_zero() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	let zero = Uint::from(0);
	shape.set_uint(&mut row, 0, &zero);
	assert!(row.is_defined(0));

	let retrieved = shape.get_uint(&row, 0);
	assert!(retrieved.is_zero());
}

#[test]
fn test_try_get() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	assert_eq!(shape.try_get_uint(&row, 0), None);

	let value = Uint::from(12345u64);
	shape.set_uint(&mut row, 0, &value);
	assert_eq!(shape.try_get_uint(&row, 0), Some(value));
}

#[test]
fn test_clone_on_write() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let row1 = shape.allocate();
	let mut row2 = row1.clone();

	let value = Uint::from(999999999999999u64);
	shape.set_uint(&mut row2, 0, &value);

	assert!(!row1.is_defined(0));
	assert!(row2.is_defined(0));
	assert_ne!(row1.as_ptr(), row2.as_ptr());
	assert_eq!(shape.get_uint(&row2, 0), value);
}

#[test]
fn test_multiple_fields() {
	let shape = RowShape::testing(&[
		ValueType::Boolean,
		ValueType::Uint,
		ValueType::Utf8,
		ValueType::Uint,
		ValueType::Int4,
	]);
	let mut row = shape.allocate();

	shape.set::<bool>(&mut row, 0, true);

	let small = Uint::from(100u64);
	shape.set_uint(&mut row, 1, &small);

	shape.set_utf8(&mut row, 2, "test");

	let large = Uint::from(u128::MAX >> 1);
	shape.set_uint(&mut row, 3, &large);

	shape.set::<i32>(&mut row, 4, 42i32);

	assert_eq!(shape.get::<bool>(&row, 0), true);
	assert_eq!(shape.get_uint(&row, 1), small);
	assert_eq!(shape.get_utf8(&row, 2), "test");
	assert_eq!(shape.get_uint(&row, 3), large);
	assert_eq!(shape.get::<i32>(&row, 4), 42);
}

#[test]
fn test_negative_input_handling() {
	let shape = RowShape::testing(&[ValueType::Uint]);

	// Uint wraps a signed BigInt, so a negative can be constructed; the encoder converts it
	// to zero rather than failing or storing a wrapped magnitude.
	let mut row1 = shape.allocate();
	let negative = Uint::from(-42);
	shape.set_uint(&mut row1, 0, &negative);

	let retrieved = shape.get_uint(&row1, 0);
	assert_eq!(retrieved, Uint::from(0));
}

#[test]
fn test_try_get_uint_wrong_type() {
	let shape = RowShape::testing(&[ValueType::Boolean]);
	let mut row = shape.allocate();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get_uint(&row, 0), None);
}

#[test]
fn test_update_uint_inline_to_inline() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	shape.set_uint(&mut row, 0, &Uint::from(42u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(42u64));

	shape.set_uint(&mut row, 0, &Uint::from(999u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(999u64));
}

#[test]
fn test_update_uint_inline_to_dynamic() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	shape.set_uint(&mut row, 0, &Uint::from(42u64));

	let huge = Uint::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_uint(&mut row, 0, &huge);
	assert_eq!(shape.get_uint(&row, 0), huge);
}

#[test]
fn test_update_uint_dynamic_to_inline() {
	let shape = RowShape::testing(&[ValueType::Uint]);
	let mut row = shape.allocate();

	let huge = Uint::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_uint(&mut row, 0, &huge);

	shape.set_uint(&mut row, 0, &Uint::from(42u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(42u64));
	assert_eq!(row.len(), shape.total_static_size());
}

#[test]
fn test_update_uint_with_other_dynamic_fields() {
	let shape = RowShape::testing(&[ValueType::Uint, ValueType::Utf8]);
	let mut row = shape.allocate();

	let huge = Uint::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_uint(&mut row, 0, &huge);
	shape.set_utf8(&mut row, 1, "hello");

	// Dropping the first field's dynamic bytes must rewrite the utf8 field's offset.
	shape.set_uint(&mut row, 0, &Uint::from(1u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(1u64));
	assert_eq!(shape.get_utf8(&row, 1), "hello");
}
