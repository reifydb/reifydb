// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ptr;

use num_bigint::{BigInt, BigUint};
use num_traits::ToPrimitive;
use reifydb_type::value::{r#type::Type, uint::Uint};

use crate::encoded::{row::EncodedRow, shape::RowShape};

const MODE_INLINE: u128 = 0x00000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

const INLINE_VALUE_MASK: u128 = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;

const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

impl RowShape {
	pub fn set_uint(&self, row: &mut EncodedRow, index: usize, value: &Uint) {
		let field = &self.fields()[index];
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uint);

		let unsigned_value = value.0.to_biguint().unwrap_or(BigUint::from(0u32));

		if let Some(u128_val) = unsigned_value.to_u128() {
			if u128_val < (1u128 << 127) {
				self.remove_dynamic_data(row, index);

				let packed = MODE_INLINE | (u128_val & INLINE_VALUE_MASK);
				unsafe {
					ptr::write_unaligned(
						row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut u128,
						packed.to_le(),
					);
				}
				row.set_valid(index, true);
				return;
			}
		}

		let bytes = unsigned_value.to_bytes_le();
		self.replace_dynamic_data(row, index, &bytes);
	}

	pub fn get_uint(&self, row: &EncodedRow, index: usize) -> Uint {
		let field = &self.fields()[index];
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uint);

		let packed = unsafe { (row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned() };
		let packed = u128::from_le(packed);

		let mode = packed & MODE_MASK;

		if mode == MODE_INLINE {
			let value = packed & INLINE_VALUE_MASK;

			let unsigned = BigUint::from(value);
			Uint::from(BigInt::from(unsigned))
		} else {
			let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
			let length = ((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

			let dynamic_start = self.dynamic_section_start();
			let data_bytes = &row.as_slice()[dynamic_start + offset..dynamic_start + offset + length];

			let unsigned = BigUint::from_bytes_le(data_bytes);
			Uint::from(BigInt::from(unsigned))
		}
	}

	pub fn try_get_uint(&self, row: &EncodedRow, index: usize) -> Option<Uint> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Uint {
			Some(self.get_uint(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use num_bigint::BigInt;
	use num_traits::Zero;
	use reifydb_type::value::{r#type::Type, uint::Uint};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_u64_inline() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		// Test simple unsigned value
		let small = Uint::from(42u64);
		shape.set_uint(&mut row, 0, &small);
		assert!(row.is_defined(0));

		let retrieved = shape.get_uint(&row, 0);
		assert_eq!(retrieved, small);

		// Test larger unsigned value
		let mut row2 = shape.allocate();
		let large = Uint::from(999999999999u64);
		shape.set_uint(&mut row2, 0, &large);
		assert_eq!(shape.get_uint(&row2, 0), large);
	}

	#[test]
	fn test_u128_boundary() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		// Value that needs u128 storage
		let large = Uint::from(u64::MAX);
		shape.set_uint(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = shape.get_uint(&row, 0);
		assert_eq!(retrieved, large);

		// Test max u128 that fits in 127 bits
		let mut row2 = shape.allocate();
		let max_u127 = Uint::from(u128::MAX >> 1); // 127 bits
		shape.set_uint(&mut row2, 0, &max_u127);
		assert_eq!(shape.get_uint(&row2, 0), max_u127);
	}

	#[test]
	fn test_dynamic_storage() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		// Create a value that requires dynamic storage (>127 bits)
		// Using string representation for very large numbers
		let huge = Uint::from(
			BigInt::parse_bytes(b"123456789012345678901234567890123456789012345678901234567890", 10)
				.unwrap(),
		);

		shape.set_uint(&mut row, 0, &huge);
		assert!(row.is_defined(0));

		let retrieved = shape.get_uint(&row, 0);
		assert_eq!(retrieved, huge);
	}

	#[test]
	fn test_zero() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		let zero = Uint::from(0);
		shape.set_uint(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = shape.get_uint(&row, 0);
		assert!(retrieved.is_zero());
	}

	#[test]
	fn test_try_get() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		// Undefined initially
		assert_eq!(shape.try_get_uint(&row, 0), None);

		// Set value
		let value = Uint::from(12345u64);
		shape.set_uint(&mut row, 0, &value);
		assert_eq!(shape.try_get_uint(&row, 0), Some(value));
	}

	#[test]
	fn test_clone_on_write() {
		let shape = RowShape::testing(&[Type::Uint]);
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
		let shape = RowShape::testing(&[Type::Boolean, Type::Uint, Type::Utf8, Type::Uint, Type::Int4]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		let small = Uint::from(100u64);
		shape.set_uint(&mut row, 1, &small);

		shape.set_utf8(&mut row, 2, "test");

		let large = Uint::from(u128::MAX >> 1);
		shape.set_uint(&mut row, 3, &large);

		shape.set_i32(&mut row, 4, 42);

		assert_eq!(shape.get_bool(&row, 0), true);
		assert_eq!(shape.get_uint(&row, 1), small);
		assert_eq!(shape.get_utf8(&row, 2), "test");
		assert_eq!(shape.get_uint(&row, 3), large);
		assert_eq!(shape.get_i32(&row, 4), 42);
	}

	#[test]
	fn test_negative_input_handling() {
		let shape = RowShape::testing(&[Type::Uint]);

		// Test how negative values are handled (should be converted to
		// 0 or error)
		let mut row1 = shape.allocate();
		let negative = Uint::from(-42); // This creates a negative BigInt
		shape.set_uint(&mut row1, 0, &negative);

		// Should store as 0 since Uint can't handle negative values
		let retrieved = shape.get_uint(&row1, 0);
		assert_eq!(retrieved, Uint::from(0));
	}

	#[test]
	fn test_try_get_uint_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_uint(&row, 0), None);
	}

	#[test]
	fn test_update_uint_inline_to_inline() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		shape.set_uint(&mut row, 0, &Uint::from(42u64));
		assert_eq!(shape.get_uint(&row, 0), Uint::from(42u64));

		shape.set_uint(&mut row, 0, &Uint::from(999u64));
		assert_eq!(shape.get_uint(&row, 0), Uint::from(999u64));
	}

	#[test]
	fn test_update_uint_inline_to_dynamic() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		shape.set_uint(&mut row, 0, &Uint::from(42u64));

		let huge = Uint::from(
			BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap(),
		);
		shape.set_uint(&mut row, 0, &huge);
		assert_eq!(shape.get_uint(&row, 0), huge);
	}

	#[test]
	fn test_update_uint_dynamic_to_inline() {
		let shape = RowShape::testing(&[Type::Uint]);
		let mut row = shape.allocate();

		let huge = Uint::from(
			BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap(),
		);
		shape.set_uint(&mut row, 0, &huge);

		shape.set_uint(&mut row, 0, &Uint::from(42u64));
		assert_eq!(shape.get_uint(&row, 0), Uint::from(42u64));
		assert_eq!(row.len(), shape.total_static_size());
	}

	#[test]
	fn test_update_uint_with_other_dynamic_fields() {
		let shape = RowShape::testing(&[Type::Uint, Type::Utf8]);
		let mut row = shape.allocate();

		let huge = Uint::from(
			BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap(),
		);
		shape.set_uint(&mut row, 0, &huge);
		shape.set_utf8(&mut row, 1, "hello");

		// Update uint to inline, verify utf8 still works
		shape.set_uint(&mut row, 0, &Uint::from(1u64));
		assert_eq!(shape.get_uint(&row, 0), Uint::from(1u64));
		assert_eq!(shape.get_utf8(&row, 1), "hello");
	}
}
