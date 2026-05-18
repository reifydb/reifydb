// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ptr;

use num_bigint::BigInt as StdBigInt;
use num_traits::ToPrimitive;
use reifydb_type::value::{int::Int, r#type::Type};

use crate::encoded::{row::EncodedRow, shape::RowShape};

const MODE_INLINE: u128 = 0x00000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

const INLINE_VALUE_MASK: u128 = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;

const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

impl RowShape {
	pub fn set_int(&self, row: &mut EncodedRow, index: usize, value: &Int) {
		let field = &self.fields()[index];
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Int);

		if let Some(i128_val) = value.0.to_i128()
			&& (-(1i128 << 126)..(1i128 << 126)).contains(&i128_val)
		{
			self.remove_dynamic_data(row, index);

			let packed = MODE_INLINE | ((i128_val as u128) & INLINE_VALUE_MASK);
			unsafe {
				ptr::write_unaligned(
					row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut u128,
					packed.to_le(),
				);
			}
			row.set_valid(index, true);
			return;
		}

		let bytes = value.0.to_signed_bytes_le();
		self.replace_dynamic_data(row, index, &bytes);
	}

	pub fn get_int(&self, row: &EncodedRow, index: usize) -> Int {
		let field = &self.fields()[index];
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Int);

		let packed = unsafe { (row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned() };
		let packed = u128::from_le(packed);

		let mode = packed & MODE_MASK;

		if mode == MODE_INLINE {
			let value = (packed & INLINE_VALUE_MASK) as i128;
			let signed = if value & (1i128 << 126) != 0 {
				value | (1i128 << 127)
			} else {
				value
			};
			Int::from(signed)
		} else {
			let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
			let length = ((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

			let dynamic_start = self.dynamic_section_start();
			let bigint_bytes = &row.as_slice()[dynamic_start + offset..dynamic_start + offset + length];

			Int::from(StdBigInt::from_signed_bytes_le(bigint_bytes))
		}
	}

	pub fn try_get_int(&self, row: &EncodedRow, index: usize) -> Option<Int> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Int {
			Some(self.get_int(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use num_bigint::BigInt;
	use reifydb_type::value::{int::Int, r#type::Type};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_i64_inline() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		// Test small positive value
		let small = Int::from(42i64);
		shape.set_int(&mut row, 0, &small);
		assert!(row.is_defined(0));

		let retrieved = shape.get_int(&row, 0);
		assert_eq!(retrieved, small);

		// Test small negative value
		let mut row2 = shape.allocate();
		let negative = Int::from(-999999i64);
		shape.set_int(&mut row2, 0, &negative);
		assert_eq!(shape.get_int(&row2, 0), negative);
	}

	#[test]
	fn test_i128_boundary() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		// Value that doesn't fit in 62 bits but fits in i128
		let large = Int::from(i64::MAX);
		shape.set_int(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = shape.get_int(&row, 0);
		assert_eq!(retrieved, large);

		// Test i128::MAX
		let mut row2 = shape.allocate();
		let max_i128 = Int::from(i128::MAX);
		shape.set_int(&mut row2, 0, &max_i128);
		assert_eq!(shape.get_int(&row2, 0), max_i128);

		// Test i128::MIN
		let mut row3 = shape.allocate();
		let min_i128 = Int::from(i128::MIN);
		shape.set_int(&mut row3, 0, &min_i128);
		assert_eq!(shape.get_int(&row3, 0), min_i128);
	}

	#[test]
	fn test_dynamic_storage() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		// Create a value larger than i128 can hold
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
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		let zero = Int::from(0);
		shape.set_int(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = shape.get_int(&row, 0);
		assert_eq!(retrieved, zero);
	}

	#[test]
	fn test_try_get() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		// Undefined initially
		assert_eq!(shape.try_get_int(&row, 0), None);

		// Set value
		let value = Int::from(12345);
		shape.set_int(&mut row, 0, &value);
		assert_eq!(shape.try_get_int(&row, 0), Some(value));
	}

	#[test]
	fn test_clone_on_write() {
		let shape = RowShape::testing(&[Type::Int]);
		let row1 = shape.allocate();
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
		let shape = RowShape::testing(&[Type::Int4, Type::Int, Type::Utf8, Type::Int]);
		let mut row = shape.allocate();

		shape.set_i32(&mut row, 0, 42);

		let small = Int::from(100);
		shape.set_int(&mut row, 1, &small);

		shape.set_utf8(&mut row, 2, "test");

		let large = Int::from(i128::MAX);
		shape.set_int(&mut row, 3, &large);

		assert_eq!(shape.get_i32(&row, 0), 42);
		assert_eq!(shape.get_int(&row, 1), small);
		assert_eq!(shape.get_utf8(&row, 2), "test");
		assert_eq!(shape.get_int(&row, 3), large);
	}

	#[test]
	fn test_negative_values() {
		let shape = RowShape::testing(&[Type::Int]);

		// Small negative (i64 inline)
		let mut row1 = shape.allocate();
		let small_neg = Int::from(-42);
		shape.set_int(&mut row1, 0, &small_neg);
		assert_eq!(shape.get_int(&row1, 0), small_neg);

		// Large negative (i128 overflow)
		let mut row2 = shape.allocate();
		let large_neg = Int::from(i64::MIN);
		shape.set_int(&mut row2, 0, &large_neg);
		assert_eq!(shape.get_int(&row2, 0), large_neg);

		// Huge negative (dynamic)
		let mut row3 = shape.allocate();
		let huge_neg_str = "-999999999999999999999999999999999999999999999999";
		let huge_neg =
			Int::from(-BigInt::parse_bytes(huge_neg_str.trim_start_matches('-').as_bytes(), 10).unwrap());
		shape.set_int(&mut row3, 0, &huge_neg);
		assert_eq!(shape.get_int(&row3, 0), huge_neg);
	}

	#[test]
	fn test_try_get_int_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_int(&row, 0), None);
	}

	#[test]
	fn test_update_int_inline_to_inline() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		shape.set_int(&mut row, 0, &Int::from(42));
		assert_eq!(shape.get_int(&row, 0), Int::from(42));

		shape.set_int(&mut row, 0, &Int::from(-999));
		assert_eq!(shape.get_int(&row, 0), Int::from(-999));
	}

	#[test]
	fn test_update_int_inline_to_dynamic() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		shape.set_int(&mut row, 0, &Int::from(42));
		assert_eq!(shape.get_int(&row, 0), Int::from(42));

		let huge = Int::from(
			BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap(),
		);
		shape.set_int(&mut row, 0, &huge);
		assert_eq!(shape.get_int(&row, 0), huge);
	}

	#[test]
	fn test_update_int_dynamic_to_inline() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		let huge = Int::from(
			BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap(),
		);
		shape.set_int(&mut row, 0, &huge);
		assert_eq!(shape.get_int(&row, 0), huge);

		// Transition back to inline
		shape.set_int(&mut row, 0, &Int::from(42));
		assert_eq!(shape.get_int(&row, 0), Int::from(42));
		// Dynamic data should be cleaned up
		assert_eq!(row.len(), shape.total_static_size());
	}

	#[test]
	fn test_update_int_dynamic_to_dynamic() {
		let shape = RowShape::testing(&[Type::Int]);
		let mut row = shape.allocate();

		let huge1 = Int::from(
			BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap(),
		);
		shape.set_int(&mut row, 0, &huge1);
		assert_eq!(shape.get_int(&row, 0), huge1);

		let huge2 = Int::from(
			-BigInt::parse_bytes(b"111111111111111111111111111111111111111111111111", 10).unwrap(),
		);
		shape.set_int(&mut row, 0, &huge2);
		assert_eq!(shape.get_int(&row, 0), huge2);
	}

	#[test]
	fn test_update_int_with_other_dynamic_fields() {
		let shape = RowShape::testing(&[Type::Int, Type::Utf8, Type::Int]);
		let mut row = shape.allocate();

		let huge1 = Int::from(
			BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap(),
		);
		shape.set_int(&mut row, 0, &huge1);
		shape.set_utf8(&mut row, 1, "hello");
		let huge2 = Int::from(
			BigInt::parse_bytes(b"111111111111111111111111111111111111111111111111", 10).unwrap(),
		);
		shape.set_int(&mut row, 2, &huge2);

		// Update first int to inline (removes dynamic data, adjusts other refs)
		shape.set_int(&mut row, 0, &Int::from(42));

		assert_eq!(shape.get_int(&row, 0), Int::from(42));
		assert_eq!(shape.get_utf8(&row, 1), "hello");
		assert_eq!(shape.get_int(&row, 2), huge2);
	}
}
