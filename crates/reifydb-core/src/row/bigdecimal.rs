// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use bigdecimal::{BigDecimal as StdBigDecimal, ToPrimitive};
use num_bigint::BigInt as StdBigInt;
use reifydb_type::{BigDecimal, Type};

use crate::row::{EncodedRow, EncodedRowLayout};

/// BigDecimal storage modes using MSB of i128 as indicator
/// MSB = 0: Scale + mantissa stored inline in lower 127 bits
/// MSB = 1: Dynamic storage, lower 127 bits contain offset+length
const MODE_INLINE: u128 = 0x00000000000000000000000000000000;
const MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

/// Bit masks for inline mode (127 bits total: 8 bits scale + 119 bits mantissa)
const INLINE_SCALE_BITS: u32 = 8; // 8 bits for scale (-128 to +127)
const INLINE_SCALE_MASK: u128 = 0x00000000000000000000000000FF; // Lower 8 bits
const INLINE_MANTISSA_MASK: u128 =
	0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF >> INLINE_SCALE_BITS; // Upper 119 bits

/// Bit masks for dynamic mode (lower 127 bits contain offset+length)
const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF; // 64 bits for offset
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000; // 63 bits for length

impl EncodedRowLayout {
	/// Set a BigDecimal value with 2-tier storage optimization
	/// - Values with scale -128..127 and mantissa fitting in 119 bits:
	///   stored inline with MSB=0
	/// - Large values: stored in dynamic section with MSB=1
	pub fn set_bigdecimal(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: &BigDecimal,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::BigDecimal);

		let (digits, scale) = value.0.as_bigint_and_exponent();

		// Try inline storage first (8-bit scale + 120-bit mantissa
		// using full i128 except MSB)
		if scale >= -128 && scale <= 127 {
			// Check if mantissa fits in i128 and won't set the MSB
			// (mode flag)
			if let Some(mantissa_i128) = digits.to_i128() {
				if mantissa_i128 >= 0
					|| (mantissa_i128 as u128) < MODE_MASK
				{
					// Mode 0: Store inline with scale in
					// lower 8 bits, mantissa in upper bits
					let scale_bits = ((scale + 128)
						as u128)
						& INLINE_SCALE_MASK; // Add bias
					let mantissa_bits = ((mantissa_i128
						as u128)
						<< INLINE_SCALE_BITS)
						& INLINE_MANTISSA_MASK;
					let packed = MODE_INLINE
						| mantissa_bits
						| scale_bits;

					unsafe {
						ptr::write_unaligned(
							row.make_mut()
								.as_mut_ptr()
								.add(field
									.offset)
								as *mut u128,
							packed.to_le(),
						);
					}
					row.set_valid(index, true);
					return;
				}
			}
		}

		// Mode 1: Dynamic storage for arbitrary precision
		debug_assert!(
			!row.is_defined(index),
			"BigDecimal field {} already set",
			index
		);

		// Serialize as scale (i64) + digits (variable bytes)
		let scale_bytes = scale.to_le_bytes();
		let digits_bytes = digits.to_signed_bytes_le();

		let dynamic_offset = self.dynamic_section_size(row);
		let total_size = 8 + digits_bytes.len();

		// Append to dynamic section
		row.0.extend_from_slice(&scale_bytes);
		row.0.extend_from_slice(&digits_bytes);

		// Pack offset and length in lower 127 bits, set MSB=1
		let offset_part =
			(dynamic_offset as u128) & DYNAMIC_OFFSET_MASK;
		let length_part =
			((total_size as u128) << 64) & DYNAMIC_LENGTH_MASK;
		let packed = MODE_DYNAMIC | offset_part | length_part;

		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut u128,
				packed.to_le(),
			);
		}
		row.set_valid(index, true);
	}

	/// Get a BigDecimal value, detecting storage mode from MSB
	pub fn get_bigdecimal(
		&self,
		row: &EncodedRow,
		index: usize,
	) -> BigDecimal {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::BigDecimal);

		let packed = unsafe {
			(row.as_ptr().add(field.offset) as *const u128)
				.read_unaligned()
		};
		let packed = u128::from_le(packed);

		let mode = packed & MODE_MASK;

		if mode == MODE_INLINE {
			// Extract 8-bit scale and remove bias
			let scale_bits = packed & INLINE_SCALE_MASK;
			let scale = (scale_bits as i64) - 128; // Remove bias

			// Extract mantissa from upper bits and convert back to
			// i128
			let mantissa_bits = (packed & INLINE_MANTISSA_MASK)
				>> INLINE_SCALE_BITS;
			let mantissa_i128 = mantissa_bits as i128;
			let mantissa = StdBigInt::from(mantissa_i128);

			BigDecimal::from(StdBigDecimal::new(mantissa, scale))
		} else {
			// MODE_DYNAMIC: Extract offset and length for dynamic
			// storage
			let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
			let length =
				((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

			let dynamic_start = self.dynamic_section_start();
			let data_bytes = &row.as_slice()[dynamic_start + offset
				..dynamic_start + offset + length];

			// Parse scale (first 8 bytes)
			let scale = i64::from_le_bytes(
				data_bytes[0..8].try_into().unwrap(),
			);

			// Parse digits (remaining bytes)
			let digits = StdBigInt::from_signed_bytes_le(
				&data_bytes[8..],
			);

			BigDecimal::from(StdBigDecimal::new(digits, scale))
		}
	}

	/// Try to get a BigDecimal value, returning None if undefined
	pub fn try_get_bigdecimal(
		&self,
		row: &EncodedRow,
		index: usize,
	) -> Option<BigDecimal> {
		if row.is_defined(index) {
			Some(self.get_bigdecimal(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {

	use reifydb_type::{BigDecimal, Type};

	use crate::row::EncodedRowLayout;

	#[test]
	fn test_bigdecimal_compact_inline() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);
		let mut row = layout.allocate_row();

		// Test simple decimal
		let decimal = BigDecimal::from_str("123.45").unwrap();
		layout.set_bigdecimal(&mut row, 0, &decimal);
		assert!(row.is_defined(0));

		let retrieved = layout.get_bigdecimal(&row, 0);
		assert_eq!(retrieved.to_string(), "123.45");

		// Test negative decimal
		let mut row2 = layout.allocate_row();
		let negative = BigDecimal::from_str("-999.99").unwrap();
		layout.set_bigdecimal(&mut row2, 0, &negative);
		assert_eq!(
			layout.get_bigdecimal(&row2, 0).to_string(),
			"-999.99"
		);
	}

	#[test]
	fn test_bigdecimal_compact_boundaries() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);

		// Test max scale (31)
		let mut row1 = layout.allocate_row();
		let high_precision = BigDecimal::from_str(
			"1.0000000000000000000000000000001",
		)
		.unwrap();
		layout.set_bigdecimal(&mut row1, 0, &high_precision);
		let retrieved = layout.get_bigdecimal(&row1, 0);
		assert_eq!(
			retrieved.to_string(),
			"1.0000000000000000000000000000001"
		);

		// Test min scale (-32)
		let mut row2 = layout.allocate_row();
		let large_int = BigDecimal::from_str(
			"100000000000000000000000000000000",
		)
		.unwrap();
		layout.set_bigdecimal(&mut row2, 0, &large_int);
		assert_eq!(
			layout.get_bigdecimal(&row2, 0).to_string(),
			"100000000000000000000000000000000"
		);
	}

	#[test]
	fn test_bigdecimal_extended_i128() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);
		let mut row = layout.allocate_row();

		// Value that needs i128 mantissa
		let large =
			BigDecimal::from_str("999999999999999999999.123456789")
				.unwrap();
		layout.set_bigdecimal(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = layout.get_bigdecimal(&row, 0);
		assert_eq!(
			retrieved.to_string(),
			"999999999999999999999.123456789"
		);
	}

	#[test]
	fn test_bigdecimal_dynamic_storage() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);
		let mut row = layout.allocate_row();

		// Create a value with huge precision
		let huge = BigDecimal::from_str(
			"123456789012345678901234567890123456789012345678901234567890.123456789",
		)
		.unwrap();

		layout.set_bigdecimal(&mut row, 0, &huge);
		assert!(row.is_defined(0));

		let retrieved = layout.get_bigdecimal(&row, 0);
		assert_eq!(
			retrieved.to_string(),
			"123456789012345678901234567890123456789012345678901234567890.123456789"
		);
	}

	#[test]
	fn test_bigdecimal_zero() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);
		let mut row = layout.allocate_row();

		let zero = BigDecimal::from_str("0.0").unwrap();
		layout.set_bigdecimal(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = layout.get_bigdecimal(&row, 0);
		assert!(retrieved.is_zero());
	}

	#[test]
	fn test_bigdecimal_currency_values() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);

		// Test typical currency value (2 decimal places)
		let mut row1 = layout.allocate_row();
		let price = BigDecimal::from_str("19.99").unwrap();
		layout.set_bigdecimal(&mut row1, 0, &price);
		assert_eq!(
			layout.get_bigdecimal(&row1, 0).to_string(),
			"19.99"
		);

		// Test large currency value
		let mut row2 = layout.allocate_row();
		let large_price = BigDecimal::from_str("999999999.99").unwrap();
		layout.set_bigdecimal(&mut row2, 0, &large_price);
		assert_eq!(
			layout.get_bigdecimal(&row2, 0).to_string(),
			"999999999.99"
		);

		// Test small fraction
		let mut row3 = layout.allocate_row();
		let fraction = BigDecimal::from_str("0.00000001").unwrap();
		layout.set_bigdecimal(&mut row3, 0, &fraction);
		assert_eq!(layout.get_bigdecimal(&row3, 0), fraction);
	}

	#[test]
	fn test_bigdecimal_scientific_notation() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);
		let mut row = layout.allocate_row();

		let scientific = BigDecimal::from_str("1.23456e10").unwrap();
		layout.set_bigdecimal(&mut row, 0, &scientific);

		let retrieved = layout.get_bigdecimal(&row, 0);
		assert_eq!(retrieved.to_string(), "12345600000");
	}

	#[test]
	fn test_bigdecimal_try_get() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);
		let mut row = layout.allocate_row();

		// Undefined initially
		assert_eq!(layout.try_get_bigdecimal(&row, 0), None);

		// Set value
		let value = BigDecimal::from_str("42.42").unwrap();
		layout.set_bigdecimal(&mut row, 0, &value);

		let retrieved = layout.try_get_bigdecimal(&row, 0);
		assert!(retrieved.is_some());
		assert_eq!(retrieved.unwrap().to_string(), "42.42");
	}

	#[test]
	fn test_bigdecimal_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		let value = BigDecimal::from_str("3.14159").unwrap();
		layout.set_bigdecimal(&mut row2, 0, &value);

		assert!(!row1.is_defined(0));
		assert!(row2.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
		assert_eq!(
			layout.get_bigdecimal(&row2, 0).to_string(),
			"3.14159"
		);
	}

	#[test]
	fn test_bigdecimal_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[
			Type::Bool,
			Type::BigDecimal,
			Type::Utf8,
			Type::BigDecimal,
			Type::Int4,
		]);
		let mut row = layout.allocate_row();

		layout.set_bool(&mut row, 0, true);

		let small_decimal = BigDecimal::from_str("99.99").unwrap();
		layout.set_bigdecimal(&mut row, 1, &small_decimal);

		layout.set_utf8(&mut row, 2, "test");

		let large_decimal =
			BigDecimal::from_str("123456789.987654321").unwrap();
		layout.set_bigdecimal(&mut row, 3, &large_decimal);

		layout.set_i32(&mut row, 4, -42);

		assert_eq!(layout.get_bool(&row, 0), true);
		assert_eq!(layout.get_bigdecimal(&row, 1).to_string(), "99.99");
		assert_eq!(layout.get_utf8(&row, 2), "test");
		assert_eq!(
			layout.get_bigdecimal(&row, 3).to_string(),
			"123456789.987654321"
		);
		assert_eq!(layout.get_i32(&row, 4), -42);
	}

	#[test]
	fn test_bigdecimal_negative_values() {
		let layout = EncodedRowLayout::new(&[Type::BigDecimal]);

		// Small negative (compact inline)
		let mut row1 = layout.allocate_row();
		let small_neg = BigDecimal::from_str("-0.01").unwrap();
		layout.set_bigdecimal(&mut row1, 0, &small_neg);
		assert_eq!(
			layout.get_bigdecimal(&row1, 0).to_string(),
			"-0.01"
		);

		// Large negative (extended i128)
		let mut row2 = layout.allocate_row();
		let large_neg = BigDecimal::from_str("-999999999999999999.999")
			.unwrap();
		layout.set_bigdecimal(&mut row2, 0, &large_neg);
		assert_eq!(
			layout.get_bigdecimal(&row2, 0).to_string(),
			"-999999999999999999.999"
		);

		// Huge negative (dynamic)
		let mut row3 = layout.allocate_row();
		let huge_neg = BigDecimal::from_str(
			"-999999999999999999999999999999999999999999999.999999999",
		)
		.unwrap();
		layout.set_bigdecimal(&mut row3, 0, &huge_neg);
		assert_eq!(
			layout.get_bigdecimal(&row3, 0).to_string(),
			"-999999999999999999999999999999999999999999999.999999999"
		);
	}
}
