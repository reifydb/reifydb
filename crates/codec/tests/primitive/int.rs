// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::{constraint::precision::Precision, int::Int, value_type::ValueType};

const NARROW: ValueType = ValueType::int(Precision::new(38));
const WIDE: ValueType = ValueType::int(Precision::new(76));
const MAX_38: &str = "99999999999999999999999999999999999999";

fn int(text: &str) -> Int {
	text.parse().expect("valid int literal")
}

#[test]
fn test_i64_inline() {
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
	let mut row = shape.allocate_pod();

	// Both signs must survive the 16 byte slot, which stores the two's complement i128.
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
	// The 16 byte slot must hold the full 38 digit range of both signs, and i128::MAX and MIN
	// (39 digits) need the 32 byte slot of a wider column without losing their sign.
	let narrow = RowShape::testing(RowFamily::Pod, &[NARROW]);
	for value in [int(MAX_38), int(&format!("-{MAX_38}")), Int::from(i64::MAX)] {
		let mut row = narrow.allocate_pod();
		narrow.set_int(&mut row, 0, &value);
		assert!(row.is_defined(0));
		assert_eq!(narrow.get_int(&row, 0), value);
	}

	let wide = RowShape::testing(RowFamily::Pod, &[WIDE]);
	for value in [Int::from(i128::MAX), Int::from(i128::MIN)] {
		let mut row = wide.allocate_pod();
		wide.set_int(&mut row, 0, &value);
		assert_eq!(wide.get_int(&row, 0), value);
	}
}

#[test]
fn test_dynamic_storage() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();

	// Far past i128 the value still lives in the fixed 32 byte slot; a row that grew would mean it spilled.
	let huge_str = "999999999999999999999999999999999999999999999999";
	let huge = int(huge_str);

	shape.set_int(&mut row, 0, &huge);
	assert!(row.is_defined(0));
	assert_eq!(row.len(), shape.total_static_size());

	let retrieved = shape.get_int(&row, 0);
	assert_eq!(retrieved, huge);
	assert_eq!(retrieved.to_string(), huge_str);
}

#[test]
fn test_slot_width_follows_precision() {
	// Precision 38 is the widest that fits i128; one more digit must switch to the 32 byte slot.
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[NARROW, ValueType::int(Precision::new(39)), WIDE, ValueType::INT, ValueType::int(Precision::new(1))],
	);
	let sizes: Vec<u32> = shape.fields().iter().map(|field| field.size).collect();
	assert_eq!(sizes, vec![16, 32, 32, 32, 16]);
}

#[test]
fn test_extremes_of_the_wide_slot() {
	// Int::MAX and Int::MIN use all 76 digits, so any truncation of the upper 16 bytes shows up here.
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	for value in [Int::MAX, Int::MIN] {
		let mut row = shape.allocate_pod();
		shape.set_int(&mut row, 0, &value);
		assert_eq!(shape.get_int(&row, 0), value);
	}
}

#[test]
#[should_panic(expected = "does not fit")]
fn test_set_past_precision_panics() {
	// A value with more digits than the column precision must never be written silently truncated.
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::int(Precision::new(3))]);
	let mut row = shape.allocate_pod();
	shape.set_int(&mut row, 0, &Int::from(1000));
}

#[test]
fn test_zero() {
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
	let mut row = shape.allocate_pod();

	let zero = Int::from(0);
	shape.set_int(&mut row, 0, &zero);
	assert!(row.is_defined(0));

	let retrieved = shape.get_int(&row, 0);
	assert_eq!(retrieved, zero);
}

#[test]
fn test_try_get() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::INT]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get_int(&row, 0), None);

	let value = Int::from(12345);
	shape.set_int(&mut row, 0, &value);
	assert_eq!(shape.try_get_int(&row, 0), Some(value));
}

#[test]
fn test_clone_on_write() {
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
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
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int4, NARROW, ValueType::Utf8, WIDE]);
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
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);

	// The sign must survive in both halves of the 32 byte slot: small, i64 sized and far past i128.
	let mut row1 = shape.allocate_pod();
	let small_neg = Int::from(-42);
	shape.set_int(&mut row1, 0, &small_neg);
	assert_eq!(shape.get_int(&row1, 0), small_neg);

	let mut row2 = shape.allocate_pod();
	let large_neg = Int::from(i64::MIN);
	shape.set_int(&mut row2, 0, &large_neg);
	assert_eq!(shape.get_int(&row2, 0), large_neg);

	let mut row3 = shape.allocate_pod();
	let huge_neg = int("-999999999999999999999999999999999999999999999999");
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
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
	let mut row = shape.allocate_pod();

	shape.set_int(&mut row, 0, &Int::from(42));
	assert_eq!(shape.get_int(&row, 0), Int::from(42));

	shape.set_int(&mut row, 0, &Int::from(-999));
	assert_eq!(shape.get_int(&row, 0), Int::from(-999));
}

#[test]
fn test_update_int_small_to_huge() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();

	shape.set_int(&mut row, 0, &Int::from(42));
	assert_eq!(shape.get_int(&row, 0), Int::from(42));

	// A wider value overwrites the whole slot in place; the row must not grow.
	let huge = int("999999999999999999999999999999999999999999999999");
	shape.set_int(&mut row, 0, &huge);
	assert_eq!(shape.get_int(&row, 0), huge);
	assert_eq!(row.len(), shape.total_static_size());
}

#[test]
fn test_update_int_huge_to_small() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();

	let huge = int("-999999999999999999999999999999999999999999999999");
	shape.set_int(&mut row, 0, &huge);
	assert_eq!(shape.get_int(&row, 0), huge);

	// Stale upper bytes of the old negative value would sign-corrupt the new one.
	shape.set_int(&mut row, 0, &Int::from(42));
	assert_eq!(shape.get_int(&row, 0), Int::from(42));
	assert_eq!(row.len(), shape.total_static_size());
}

#[test]
fn test_update_int_huge_to_huge() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();

	let huge1 = int("999999999999999999999999999999999999999999999999");
	shape.set_int(&mut row, 0, &huge1);
	assert_eq!(shape.get_int(&row, 0), huge1);

	let huge2 = int("-111111111111111111111111111111111111111111111111");
	shape.set_int(&mut row, 0, &huge2);
	assert_eq!(shape.get_int(&row, 0), huge2);
}

#[test]
fn test_update_int_with_other_dynamic_fields() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE, ValueType::Utf8, WIDE]);
	let mut row = shape.allocate_pod();

	let huge1 = int("999999999999999999999999999999999999999999999999");
	shape.set_int(&mut row, 0, &huge1);
	shape.set_utf8(&mut row, 1, "hello");
	let huge2 = int("111111111111111111111111111111111111111111111111");
	shape.set_int(&mut row, 2, &huge2);

	// Rewriting an int slot must leave the utf8 field's dynamic bytes and the other slot untouched.
	shape.set_int(&mut row, 0, &Int::from(42));

	assert_eq!(shape.get_int(&row, 0), Int::from(42));
	assert_eq!(shape.get_utf8(&row, 1), "hello");
	assert_eq!(shape.get_int(&row, 2), huge2);
}
