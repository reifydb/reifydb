// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use num_bigint::BigUint;
use num_traits::ToPrimitive;
use reifydb_type::{Type, Uint};

use crate::value::encoded::{EncodedValues, EncodedValuesLayout};

/// Uint storage modes using MSB of u128 as indicator
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

impl EncodedValuesLayout {
	/// Set a Uint value with 2-tier storage optimization
	/// - Values fitting in 127 bits: stored inline with MSB=0
	/// - Large values: stored in dynamic section with MSB=1
	pub fn set_uint(&self, row: &mut EncodedValues, index: usize, value: &Uint) {
		let field = &self.fields[index];
		debug_assert_eq!(field.r#type, Type::Uint);

		// Uint should already be non-negative, but let's ensure it
		let unsigned_value = value.0.to_biguint().unwrap_or(BigUint::from(0u32));

		// Try u128 inline storage first (fits in 127 bits)
		if let Some(u128_val) = unsigned_value.to_u128() {
			// Check if value fits in 127 bits (MSB must be 0)
			if u128_val < (1u128 << 127) {
				// Mode 0: Store inline in lower 127 bits
				let packed = MODE_INLINE | (u128_val & INLINE_VALUE_MASK);
				unsafe {
					ptr::write_unaligned(
						row.make_mut().as_mut_ptr().add(field.offset) as *mut u128,
						packed.to_le(),
					);
				}
				row.set_valid(index, true);
				return;
			}
		}

		// Mode 1: Dynamic storage for arbitrary precision
		debug_assert!(!row.is_defined(index), "Uint field {} already set", index);

		// Serialize as unsigned bytes
		let bytes = unsigned_value.to_bytes_le();

		let dynamic_offset = self.dynamic_section_size(row);
		let total_size = bytes.len();

		// Append to dynamic section
		row.0.extend_from_slice(&bytes);

		// Pack offset and length in lower 127 bits, set MSB=1
		let offset_part = (dynamic_offset as u128) & DYNAMIC_OFFSET_MASK;
		let length_part = ((total_size as u128) << 64) & DYNAMIC_LENGTH_MASK;
		let packed = MODE_DYNAMIC | offset_part | length_part;

		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset) as *mut u128,
				packed.to_le(),
			);
		}
		row.set_valid(index, true);
	}

	/// Get a Uint value, detecting storage mode from MSB
	pub fn get_uint(&self, row: &EncodedValues, index: usize) -> Uint {
		let field = &self.fields[index];
		debug_assert_eq!(field.r#type, Type::Uint);

		let packed = unsafe { (row.as_ptr().add(field.offset) as *const u128).read_unaligned() };
		let packed = u128::from_le(packed);

		let mode = packed & MODE_MASK;

		if mode == MODE_INLINE {
			// Extract value from lower 127 bits
			let value = packed & INLINE_VALUE_MASK;
			// Convert to BigUint then to Uint
			let unsigned = BigUint::from(value);
			Uint::from(num_bigint::BigInt::from(unsigned))
		} else {
			// MODE_DYNAMIC: Extract offset and length for dynamic
			// storage
			let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
			let length = ((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

			let dynamic_start = self.dynamic_section_start();
			let data_bytes = &row.as_slice()[dynamic_start + offset..dynamic_start + offset + length];

			// Parse as unsigned bytes
			let unsigned = BigUint::from_bytes_le(data_bytes);
			Uint::from(num_bigint::BigInt::from(unsigned))
		}
	}

	/// Try to get a Uint value, returning None if undefined
	pub fn try_get_uint(&self, row: &EncodedValues, index: usize) -> Option<Uint> {
		if row.is_defined(index) {
			Some(self.get_uint(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use num_traits::Zero;
	use reifydb_type::{Type, Uint};

	use crate::value::encoded::EncodedValuesLayout;

	#[test]
	fn test_u64_inline() {
		let layout = EncodedValuesLayout::new(&[Type::Uint]);
		let mut row = layout.allocate();

		// Test simple unsigned value
		let small = Uint::from(42u64);
		layout.set_uint(&mut row, 0, &small);
		assert!(row.is_defined(0));

		let retrieved = layout.get_uint(&row, 0);
		assert_eq!(retrieved, small);

		// Test larger unsigned value
		let mut row2 = layout.allocate();
		let large = Uint::from(999999999999u64);
		layout.set_uint(&mut row2, 0, &large);
		assert_eq!(layout.get_uint(&row2, 0), large);
	}

	#[test]
	fn test_u128_boundary() {
		let layout = EncodedValuesLayout::new(&[Type::Uint]);
		let mut row = layout.allocate();

		// Value that needs u128 storage
		let large = Uint::from(u64::MAX);
		layout.set_uint(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = layout.get_uint(&row, 0);
		assert_eq!(retrieved, large);

		// Test max u128 that fits in 127 bits
		let mut row2 = layout.allocate();
		let max_u127 = Uint::from(u128::MAX >> 1); // 127 bits
		layout.set_uint(&mut row2, 0, &max_u127);
		assert_eq!(layout.get_uint(&row2, 0), max_u127);
	}

	#[test]
	fn test_dynamic_storage() {
		let layout = EncodedValuesLayout::new(&[Type::Uint]);
		let mut row = layout.allocate();

		// Create a value that requires dynamic storage (>127 bits)
		// Using string representation for very large numbers
		let huge = Uint::from(
			num_bigint::BigInt::parse_bytes(
				b"123456789012345678901234567890123456789012345678901234567890",
				10,
			)
			.unwrap(),
		);

		layout.set_uint(&mut row, 0, &huge);
		assert!(row.is_defined(0));

		let retrieved = layout.get_uint(&row, 0);
		assert_eq!(retrieved, huge);
	}

	#[test]
	fn test_zero() {
		let layout = EncodedValuesLayout::new(&[Type::Uint]);
		let mut row = layout.allocate();

		let zero = Uint::from(0);
		layout.set_uint(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = layout.get_uint(&row, 0);
		assert!(retrieved.is_zero());
	}

	#[test]
	fn test_try_get() {
		let layout = EncodedValuesLayout::new(&[Type::Uint]);
		let mut row = layout.allocate();

		// Undefined initially
		assert_eq!(layout.try_get_uint(&row, 0), None);

		// Set value
		let value = Uint::from(12345u64);
		layout.set_uint(&mut row, 0, &value);
		assert_eq!(layout.try_get_uint(&row, 0), Some(value));
	}

	#[test]
	fn test_clone_on_write() {
		let layout = EncodedValuesLayout::new(&[Type::Uint]);
		let row1 = layout.allocate();
		let mut row2 = row1.clone();

		let value = Uint::from(999999999999999u64);
		layout.set_uint(&mut row2, 0, &value);

		assert!(!row1.is_defined(0));
		assert!(row2.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
		assert_eq!(layout.get_uint(&row2, 0), value);
	}

	#[test]
	fn test_multiple_fields() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean, Type::Uint, Type::Utf8, Type::Uint, Type::Int4]);
		let mut row = layout.allocate();

		layout.set_bool(&mut row, 0, true);

		let small = Uint::from(100u64);
		layout.set_uint(&mut row, 1, &small);

		layout.set_utf8(&mut row, 2, "test");

		let large = Uint::from(u128::MAX >> 1);
		layout.set_uint(&mut row, 3, &large);

		layout.set_i32(&mut row, 4, 42);

		assert_eq!(layout.get_bool(&row, 0), true);
		assert_eq!(layout.get_uint(&row, 1), small);
		assert_eq!(layout.get_utf8(&row, 2), "test");
		assert_eq!(layout.get_uint(&row, 3), large);
		assert_eq!(layout.get_i32(&row, 4), 42);
	}

	#[test]
	fn test_negative_input_handling() {
		let layout = EncodedValuesLayout::new(&[Type::Uint]);

		// Test how negative values are handled (should be converted to
		// 0 or error)
		let mut row1 = layout.allocate();
		let negative = Uint::from(-42); // This creates a negative BigInt
		layout.set_uint(&mut row1, 0, &negative);

		// Should store as 0 since Uint can't handle negative values
		let retrieved = layout.get_uint(&row1, 0);
		assert_eq!(retrieved, Uint::from(0));
	}
}
