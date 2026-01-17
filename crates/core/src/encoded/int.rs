// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use num_bigint::BigInt as StdBigInt;
use num_traits::ToPrimitive;
use reifydb_type::value::{int::Int, r#type::Type};

use crate::encoded::{encoded::EncodedValues, schema::Schema};

/// Int storage modes using MSB of i128 as indicator
/// MSB = 0: Value stored inline in lower 127 bits
/// MSB = 1: Dynamic storage, lower 127 bits contain offset+length
const MODE_INLINE: u128 = 0x00000000000000000000000000000000;
const MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

/// Bit masks for inline mode (127 bits for value)
const INLINE_VALUE_MASK: u128 = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;

/// Bit masks for dynamic mode (lower 127 bits contain offset+length)
const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF; // 64 bits for offset
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000; // 63 bits for length

impl Schema {
	/// Set a Int value with 2-tier storage optimization
	/// - Values fitting in 127 bits: stored inline with MSB=0
	/// - Large values: stored in dynamic section with MSB=1
	pub fn set_int(&self, row: &mut EncodedValues, index: usize, value: &Int) {
		let field = &self.fields()[index];
		debug_assert_eq!(field.constraint.get_type(), Type::Int);

		// Try i128 inline storage first (fits in 127 bits)
		if let Some(i128_val) = value.0.to_i128() {
			// Check if value fits in 127 bits (MSB must be 0)
			if i128_val >= -(1i128 << 126) && i128_val < (1i128 << 126) {
				// Mode 0: Store inline in lower 127 bits
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
		}

		// Mode 1: Dynamic storage for arbitrary precision
		debug_assert!(!row.is_defined(index), "Int field {} already set", index);

		let bytes = value.0.to_signed_bytes_le();
		let dynamic_offset = self.dynamic_section_size(row);

		// Append to dynamic section
		row.0.extend_from_slice(&bytes);

		// Pack offset and length in lower 127 bits, set MSB=1
		let offset_part = (dynamic_offset as u128) & DYNAMIC_OFFSET_MASK;
		let length_part = ((bytes.len() as u128) << 64) & DYNAMIC_LENGTH_MASK;
		let packed = MODE_DYNAMIC | offset_part | length_part;

		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut u128,
				packed.to_le(),
			);
		}
		row.set_valid(index, true);
	}

	/// Get a Int value, detecting storage mode from MSB
	pub fn get_int(&self, row: &EncodedValues, index: usize) -> Int {
		let field = &self.fields()[index];
		debug_assert_eq!(field.constraint.get_type(), Type::Int);

		let packed = unsafe { (row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned() };
		let packed = u128::from_le(packed);

		let mode = packed & MODE_MASK;

		if mode == MODE_INLINE {
			// Extract 127-bit value and sign-extend to i128
			let value = (packed & INLINE_VALUE_MASK) as i128;
			let signed = if value & (1i128 << 126) != 0 {
				// Sign bit is set, extend with 1s
				value | (1i128 << 127)
			} else {
				value
			};
			Int::from(signed)
		} else {
			// MODE_DYNAMIC: Extract offset and length for dynamic
			// storage
			let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
			let length = ((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

			let dynamic_start = self.dynamic_section_start();
			let bigint_bytes = &row.as_slice()[dynamic_start + offset..dynamic_start + offset + length];

			Int::from(StdBigInt::from_signed_bytes_le(bigint_bytes))
		}
	}

	/// Try to get a Int value, returning None if undefined
	pub fn try_get_int(&self, row: &EncodedValues, index: usize) -> Option<Int> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Int {
			Some(self.get_int(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{int::Int, r#type::Type};

	use crate::encoded::schema::Schema;

	#[test]
	fn test_i64_inline() {
		let schema = Schema::testing(&[Type::Int]);
		let mut row = schema.allocate();

		// Test small positive value
		let small = Int::from(42i64);
		schema.set_int(&mut row, 0, &small);
		assert!(row.is_defined(0));

		let retrieved = schema.get_int(&row, 0);
		assert_eq!(retrieved, small);

		// Test small negative value
		let mut row2 = schema.allocate();
		let negative = Int::from(-999999i64);
		schema.set_int(&mut row2, 0, &negative);
		assert_eq!(schema.get_int(&row2, 0), negative);
	}

	#[test]
	fn test_i128_boundary() {
		let schema = Schema::testing(&[Type::Int]);
		let mut row = schema.allocate();

		// Value that doesn't fit in 62 bits but fits in i128
		let large = Int::from(i64::MAX);
		schema.set_int(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = schema.get_int(&row, 0);
		assert_eq!(retrieved, large);

		// Test i128::MAX
		let mut row2 = schema.allocate();
		let max_i128 = Int::from(i128::MAX);
		schema.set_int(&mut row2, 0, &max_i128);
		assert_eq!(schema.get_int(&row2, 0), max_i128);

		// Test i128::MIN
		let mut row3 = schema.allocate();
		let min_i128 = Int::from(i128::MIN);
		schema.set_int(&mut row3, 0, &min_i128);
		assert_eq!(schema.get_int(&row3, 0), min_i128);
	}

	#[test]
	fn test_dynamic_storage() {
		let schema = Schema::testing(&[Type::Int]);
		let mut row = schema.allocate();

		// Create a value larger than i128 can hold
		let huge_str = "999999999999999999999999999999999999999999999999";
		let huge = Int::from(num_bigint::BigInt::parse_bytes(huge_str.as_bytes(), 10).unwrap());

		schema.set_int(&mut row, 0, &huge);
		assert!(row.is_defined(0));

		let retrieved = schema.get_int(&row, 0);
		assert_eq!(retrieved, huge);
		assert_eq!(retrieved.to_string(), huge_str);
	}

	#[test]
	fn test_zero() {
		let schema = Schema::testing(&[Type::Int]);
		let mut row = schema.allocate();

		let zero = Int::from(0);
		schema.set_int(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = schema.get_int(&row, 0);
		assert_eq!(retrieved, zero);
	}

	#[test]
	fn test_try_get() {
		let schema = Schema::testing(&[Type::Int]);
		let mut row = schema.allocate();

		// Undefined initially
		assert_eq!(schema.try_get_int(&row, 0), None);

		// Set value
		let value = Int::from(12345);
		schema.set_int(&mut row, 0, &value);
		assert_eq!(schema.try_get_int(&row, 0), Some(value));
	}

	#[test]
	fn test_clone_on_write() {
		let schema = Schema::testing(&[Type::Int]);
		let row1 = schema.allocate();
		let mut row2 = row1.clone();

		let value = Int::from(999999999999999i64);
		schema.set_int(&mut row2, 0, &value);

		assert!(!row1.is_defined(0));
		assert!(row2.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
		assert_eq!(schema.get_int(&row2, 0), value);
	}

	#[test]
	fn test_multiple_fields() {
		let schema = Schema::testing(&[Type::Int4, Type::Int, Type::Utf8, Type::Int]);
		let mut row = schema.allocate();

		schema.set_i32(&mut row, 0, 42);

		let small = Int::from(100);
		schema.set_int(&mut row, 1, &small);

		schema.set_utf8(&mut row, 2, "test");

		let large = Int::from(i128::MAX);
		schema.set_int(&mut row, 3, &large);

		assert_eq!(schema.get_i32(&row, 0), 42);
		assert_eq!(schema.get_int(&row, 1), small);
		assert_eq!(schema.get_utf8(&row, 2), "test");
		assert_eq!(schema.get_int(&row, 3), large);
	}

	#[test]
	fn test_negative_values() {
		let schema = Schema::testing(&[Type::Int]);

		// Small negative (i64 inline)
		let mut row1 = schema.allocate();
		let small_neg = Int::from(-42);
		schema.set_int(&mut row1, 0, &small_neg);
		assert_eq!(schema.get_int(&row1, 0), small_neg);

		// Large negative (i128 overflow)
		let mut row2 = schema.allocate();
		let large_neg = Int::from(i64::MIN);
		schema.set_int(&mut row2, 0, &large_neg);
		assert_eq!(schema.get_int(&row2, 0), large_neg);

		// Huge negative (dynamic)
		let mut row3 = schema.allocate();
		let huge_neg_str = "-999999999999999999999999999999999999999999999999";
		let huge_neg = Int::from(
			-num_bigint::BigInt::parse_bytes(huge_neg_str.trim_start_matches('-').as_bytes(), 10).unwrap(),
		);
		schema.set_int(&mut row3, 0, &huge_neg);
		assert_eq!(schema.get_int(&row3, 0), huge_neg);
	}

	#[test]
	fn test_try_get_int_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_int(&row, 0), None);
	}
}
