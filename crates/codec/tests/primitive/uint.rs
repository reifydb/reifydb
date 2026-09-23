// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::{constraint::precision::Precision, uint::Uint, value_type::ValueType};

const NARROW: ValueType = ValueType::uint(Precision::new(38));
const WIDE: ValueType = ValueType::uint(Precision::new(76));

fn uint(text: &str) -> Uint {
	text.parse().expect("valid uint literal")
}

#[test]
fn test_u64_inline() {
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
	let mut row = shape.allocate_pod();

	let small = Uint::from(42u64);
	shape.set_uint(&mut row, 0, &small);
	assert!(row.is_defined(0));

	let retrieved = shape.get_uint(&row, 0);
	assert_eq!(retrieved, small);

	let mut row2 = shape.allocate_pod();
	let large = Uint::from(999999999999u64);
	shape.set_uint(&mut row2, 0, &large);
	assert_eq!(shape.get_uint(&row2, 0), large);
}

#[test]
fn test_u128_boundary() {
	// The 16 byte slot holds all 38 digits; u128::MAX (39 digits) needs the 32 byte slot and
	// must not come back as a negative two's complement value.
	let narrow = RowShape::testing(RowFamily::Pod, &[NARROW]);
	for value in [Uint::from(u64::MAX), uint("99999999999999999999999999999999999999")] {
		let mut row = narrow.allocate_pod();
		narrow.set_uint(&mut row, 0, &value);
		assert!(row.is_defined(0));
		assert_eq!(narrow.get_uint(&row, 0), value);
	}

	let wide = RowShape::testing(RowFamily::Pod, &[WIDE]);
	for value in [Uint::from(u128::MAX >> 1), Uint::from(u128::MAX)] {
		let mut row = wide.allocate_pod();
		wide.set_uint(&mut row, 0, &value);
		assert_eq!(wide.get_uint(&row, 0), value);
	}
}

#[test]
fn test_dynamic_storage() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();

	// Past 2^128 the value still lives in the fixed 32 byte slot; a row that grew would mean it spilled.
	let huge = uint("123456789012345678901234567890123456789012345678901234567890");

	shape.set_uint(&mut row, 0, &huge);
	assert!(row.is_defined(0));
	assert_eq!(row.len(), shape.total_static_size());

	let retrieved = shape.get_uint(&row, 0);
	assert_eq!(retrieved, huge);
}

#[test]
fn test_slot_width_follows_precision() {
	// Precision 38 is the widest that fits i128; one more digit must switch to the 32 byte slot.
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW, ValueType::uint(Precision::new(39)), ValueType::UINT]);
	let sizes: Vec<u32> = shape.fields().iter().map(|field| field.size).collect();
	assert_eq!(sizes, vec![16, 32, 32]);
}

#[test]
fn test_max_of_the_wide_slot() {
	// Uint::MAX uses all 76 digits, so any truncation of the upper 16 bytes shows up here.
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();
	shape.set_uint(&mut row, 0, &Uint::MAX);
	assert_eq!(shape.get_uint(&row, 0), Uint::MAX);
}

#[test]
#[should_panic(expected = "does not fit")]
fn test_set_past_precision_panics() {
	// A value with more digits than the column precision must never be written silently truncated.
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::uint(Precision::new(2))]);
	let mut row = shape.allocate_pod();
	shape.set_uint(&mut row, 0, &Uint::from(100u64));
}

#[test]
fn test_zero() {
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
	let mut row = shape.allocate_pod();

	let zero = Uint::from(0u64);
	shape.set_uint(&mut row, 0, &zero);
	assert!(row.is_defined(0));

	let retrieved = shape.get_uint(&row, 0);
	assert!(retrieved.is_zero());
}

#[test]
fn test_try_get() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::UINT]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get_uint(&row, 0), None);

	let value = Uint::from(12345u64);
	shape.set_uint(&mut row, 0, &value);
	assert_eq!(shape.try_get_uint(&row, 0), Some(value));
}

#[test]
fn test_clone_on_write() {
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
	let row1 = shape.allocate_pod();
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
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[ValueType::Boolean, NARROW, ValueType::Utf8, WIDE, ValueType::Int4],
	);
	let mut row = shape.allocate_pod();

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
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);

	// A negative source clamps to zero on the way into Uint, so the row must never see a wrapped magnitude.
	let mut row1 = shape.allocate_pod();
	let negative = Uint::from(-42);
	shape.set_uint(&mut row1, 0, &negative);

	let retrieved = shape.get_uint(&row1, 0);
	assert_eq!(retrieved, Uint::from(0u64));
}

#[test]
fn test_try_get_uint_wrong_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get_uint(&row, 0), None);
}

#[test]
fn test_update_uint_inline_to_inline() {
	let shape = RowShape::testing(RowFamily::Pod, &[NARROW]);
	let mut row = shape.allocate_pod();

	shape.set_uint(&mut row, 0, &Uint::from(42u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(42u64));

	shape.set_uint(&mut row, 0, &Uint::from(999u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(999u64));
}

#[test]
fn test_update_uint_small_to_huge() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();

	shape.set_uint(&mut row, 0, &Uint::from(42u64));

	// A wider value overwrites the whole slot in place; the row must not grow.
	let huge = uint("999999999999999999999999999999999999999999999999");
	shape.set_uint(&mut row, 0, &huge);
	assert_eq!(shape.get_uint(&row, 0), huge);
	assert_eq!(row.len(), shape.total_static_size());
}

#[test]
fn test_update_uint_huge_to_small() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE]);
	let mut row = shape.allocate_pod();

	let huge = uint("999999999999999999999999999999999999999999999999");
	shape.set_uint(&mut row, 0, &huge);

	// Stale upper bytes of the old value would corrupt the new one.
	shape.set_uint(&mut row, 0, &Uint::from(42u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(42u64));
	assert_eq!(row.len(), shape.total_static_size());
}

#[test]
fn test_update_uint_with_other_dynamic_fields() {
	let shape = RowShape::testing(RowFamily::Pod, &[WIDE, ValueType::Utf8]);
	let mut row = shape.allocate_pod();

	let huge = uint("999999999999999999999999999999999999999999999999");
	shape.set_uint(&mut row, 0, &huge);
	shape.set_utf8(&mut row, 1, "hello");

	// Rewriting the uint slot must leave the utf8 field's dynamic bytes untouched.
	shape.set_uint(&mut row, 0, &Uint::from(1u64));
	assert_eq!(shape.get_uint(&row, 0), Uint::from(1u64));
	assert_eq!(shape.get_utf8(&row, 1), "hello");
}
