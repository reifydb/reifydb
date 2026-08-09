// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::BigInt;
use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::{int::Int, value_type::ValueType};

#[test]
fn test_i64_inline() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	// Both signs must survive the inline packing, which reuses the top bit as a mode flag
	// and sign-extends from bit 126 on the way back out.
	let small = Int::from(42i64);
	shape.set_int(&mut row, 0, &small);
	assert!(row.is_defined(0));

	let retrieved = shape.get_int(&row, 0);
	assert_eq!(retrieved, small);

	let mut row2 = shape.allocate_pod();
	let negative = Int::from(-999999i64);
	shape.set_int(&mut row2, 0, &negative);
	assert_eq!(shape.get_int(&row2, 0), negative);
}

#[test]
fn test_i128_boundary() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	// Inline holds magnitudes below 2^126; i64::MAX stays inline while i128::MAX and MIN
	// exceed it and must spill to the dynamic section without losing their sign.
	let large = Int::from(i64::MAX);
	shape.set_int(&mut row, 0, &large);
	assert!(row.is_defined(0));

	let retrieved = shape.get_int(&row, 0);
	assert_eq!(retrieved, large);

	let mut row2 = shape.allocate_pod();
	let max_i128 = Int::from(i128::MAX);
	shape.set_int(&mut row2, 0, &max_i128);
	assert_eq!(shape.get_int(&row2, 0), max_i128);

	let mut row3 = shape.allocate_pod();
	let min_i128 = Int::from(i128::MIN);
	shape.set_int(&mut row3, 0, &min_i128);
	assert_eq!(shape.get_int(&row3, 0), min_i128);
}

#[test]
fn test_dynamic_storage() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	// Far past i128, so the value is stored as signed little-endian bytes in the dynamic
	// section instead of the fixed slot.
	let huge_str = "999999999999999999999999999999999999999999999999";
	let huge = Int::from(BigInt::parse_bytes(huge_str.as_bytes(), 10).unwrap());

	shape.set_int(&mut row, 0, &huge);
	assert!(row.is_defined(0));

	let retrieved = shape.get_int(&row, 0);
	assert_eq!(retrieved, huge);
	assert_eq!(retrieved.to_string(), huge_str);
}

#[test]
fn test_zero() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	let zero = Int::from(0);
	shape.set_int(&mut row, 0, &zero);
	assert!(row.is_defined(0));

	let retrieved = shape.get_int(&row, 0);
	assert_eq!(retrieved, zero);
}

#[test]
fn test_try_get() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get_int(&row, 0), None);

	let value = Int::from(12345);
	shape.set_int(&mut row, 0, &value);
	assert_eq!(shape.try_get_int(&row, 0), Some(value));
}

#[test]
fn test_clone_on_write() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let row1 = shape.allocate_pod();
	let mut row2 = row1.clone();

	let value = Int::from(999999999999999i64);
	shape.set_int(&mut row2, 0, &value);

	assert!(!row1.is_defined(0));
	assert!(row2.is_defined(0));
	assert_ne!(row1.as_ptr(), row2.as_ptr());
	assert_eq!(shape.get_int(&row2, 0), value);
}

#[test]
fn test_multiple_fields() {
	let shape =
		RowShape::testing(RowFamily::Pod, &[ValueType::Int4, ValueType::Int, ValueType::Utf8, ValueType::Int]);
	let mut row = shape.allocate_pod();

	shape.set::<i32>(&mut row, 0, 42i32);

	let small = Int::from(100);
	shape.set_int(&mut row, 1, &small);

	shape.set_utf8(&mut row, 2, "test");

	let large = Int::from(i128::MAX);
	shape.set_int(&mut row, 3, &large);

	assert_eq!(shape.get::<i32>(&row, 0), 42);
	assert_eq!(shape.get_int(&row, 1), small);
	assert_eq!(shape.get_utf8(&row, 2), "test");
	assert_eq!(shape.get_int(&row, 3), large);
}

#[test]
fn test_negative_values() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);

	// The sign must survive on both sides of the inline/dynamic split: -42 and i64::MIN
	// stay inline, the last value is far past 2^126 and spills.
	let mut row1 = shape.allocate_pod();
	let small_neg = Int::from(-42);
	shape.set_int(&mut row1, 0, &small_neg);
	assert_eq!(shape.get_int(&row1, 0), small_neg);

	let mut row2 = shape.allocate_pod();
	let large_neg = Int::from(i64::MIN);
	shape.set_int(&mut row2, 0, &large_neg);
	assert_eq!(shape.get_int(&row2, 0), large_neg);

	let mut row3 = shape.allocate_pod();
	let huge_neg_str = "-999999999999999999999999999999999999999999999999";
	let huge_neg = Int::from(-BigInt::parse_bytes(huge_neg_str.trim_start_matches('-').as_bytes(), 10).unwrap());
	shape.set_int(&mut row3, 0, &huge_neg);
	assert_eq!(shape.get_int(&row3, 0), huge_neg);
}

#[test]
fn test_try_get_int_wrong_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get_int(&row, 0), None);
}

#[test]
fn test_update_int_inline_to_inline() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	shape.set_int(&mut row, 0, &Int::from(42));
	assert_eq!(shape.get_int(&row, 0), Int::from(42));

	shape.set_int(&mut row, 0, &Int::from(-999));
	assert_eq!(shape.get_int(&row, 0), Int::from(-999));
}

#[test]
fn test_update_int_inline_to_dynamic() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	shape.set_int(&mut row, 0, &Int::from(42));
	assert_eq!(shape.get_int(&row, 0), Int::from(42));

	let huge = Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_int(&mut row, 0, &huge);
	assert_eq!(shape.get_int(&row, 0), huge);
}

#[test]
fn test_update_int_dynamic_to_inline() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	let huge = Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_int(&mut row, 0, &huge);
	assert_eq!(shape.get_int(&row, 0), huge);

	// Falling back to inline must release the dynamic bytes, or the row grows on every
	// write that crosses the boundary.
	shape.set_int(&mut row, 0, &Int::from(42));
	assert_eq!(shape.get_int(&row, 0), Int::from(42));
	assert_eq!(row.len(), shape.total_static_size());
}

#[test]
fn test_update_int_dynamic_to_dynamic() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int]);
	let mut row = shape.allocate_pod();

	let huge1 = Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_int(&mut row, 0, &huge1);
	assert_eq!(shape.get_int(&row, 0), huge1);

	let huge2 = Int::from(-BigInt::parse_bytes(b"111111111111111111111111111111111111111111111111", 10).unwrap());
	shape.set_int(&mut row, 0, &huge2);
	assert_eq!(shape.get_int(&row, 0), huge2);
}

#[test]
fn test_update_int_with_other_dynamic_fields() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int, ValueType::Utf8, ValueType::Int]);
	let mut row = shape.allocate_pod();

	let huge1 = Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap());
	shape.set_int(&mut row, 0, &huge1);
	shape.set_utf8(&mut row, 1, "hello");
	let huge2 = Int::from(BigInt::parse_bytes(b"111111111111111111111111111111111111111111111111", 10).unwrap());
	shape.set_int(&mut row, 2, &huge2);

	// Dropping the first field's dynamic bytes must rewrite the later fields' offsets.
	shape.set_int(&mut row, 0, &Int::from(42));

	assert_eq!(shape.get_int(&row, 0), Int::from(42));
	assert_eq!(shape.get_utf8(&row, 1), "hello");
	assert_eq!(shape.get_int(&row, 2), huge2);
}
