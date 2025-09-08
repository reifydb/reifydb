// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use num_bigint::BigInt as StdBigInt;
use num_traits::ToPrimitive;
use reifydb_type::{Type, VarInt};

use crate::row::{EncodedRow, EncodedRowLayout};

/// VarInt storage modes using MSB of i128 as indicator
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

impl EncodedRowLayout {
	/// Set a VarInt value with 2-tier storage optimization
	/// - Values fitting in 127 bits: stored inline with MSB=0
	/// - Large values: stored in dynamic section with MSB=1
	pub fn set_varint(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: &VarInt,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::VarInt);

		// Try i128 inline storage first (fits in 127 bits)
		if let Some(i128_val) = value.0.to_i128() {
			// Check if value fits in 127 bits (MSB must be 0)
			if i128_val >= -(1i128 << 126)
				&& i128_val < (1i128 << 126)
			{
				// Mode 0: Store inline in lower 127 bits
				let packed = MODE_INLINE
					| ((i128_val as u128)
						& INLINE_VALUE_MASK);
				unsafe {
					ptr::write_unaligned(
						row.make_mut()
							.as_mut_ptr()
							.add(field.offset)
							as *mut u128,
						packed.to_le(),
					);
				}
				row.set_valid(index, true);
				return;
			}
		}

		// Mode 1: Dynamic storage for arbitrary precision
		debug_assert!(
			!row.is_defined(index),
			"VarInt field {} already set",
			index
		);

		let bytes = value.0.to_signed_bytes_le();
		let dynamic_offset = self.dynamic_section_size(row);

		// Append to dynamic section
		row.0.extend_from_slice(&bytes);

		// Pack offset and length in lower 127 bits, set MSB=1
		let offset_part =
			(dynamic_offset as u128) & DYNAMIC_OFFSET_MASK;
		let length_part =
			((bytes.len() as u128) << 64) & DYNAMIC_LENGTH_MASK;
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

	/// Get a VarInt value, detecting storage mode from MSB
	pub fn get_varint(&self, row: &EncodedRow, index: usize) -> VarInt {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::VarInt);

		let packed = unsafe {
			(row.as_ptr().add(field.offset) as *const u128)
				.read_unaligned()
		};
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
			VarInt::from(signed)
		} else {
			// MODE_DYNAMIC: Extract offset and length for dynamic
			// storage
			let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
			let length =
				((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

			let dynamic_start = self.dynamic_section_start();
			let bigint_bytes = &row.as_slice()[dynamic_start
				+ offset
				..dynamic_start + offset + length];

			VarInt::from(StdBigInt::from_signed_bytes_le(
				bigint_bytes,
			))
		}
	}

	/// Try to get a VarInt value, returning None if undefined
	pub fn try_get_varint(
		&self,
		row: &EncodedRow,
		index: usize,
	) -> Option<VarInt> {
		if row.is_defined(index) {
			Some(self.get_varint(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::{Type, VarInt};

	use crate::row::EncodedRowLayout;

	#[test]
	fn test_i64_inline() {
		let layout = EncodedRowLayout::new(&[Type::VarInt]);
		let mut row = layout.allocate_row();

		// Test small positive value
		let small = VarInt::from(42i64);
		layout.set_varint(&mut row, 0, &small);
		assert!(row.is_defined(0));

		let retrieved = layout.get_varint(&row, 0);
		assert_eq!(retrieved, small);

		// Test small negative value
		let mut row2 = layout.allocate_row();
		let negative = VarInt::from(-999999i64);
		layout.set_varint(&mut row2, 0, &negative);
		assert_eq!(layout.get_varint(&row2, 0), negative);
	}

	#[test]
	fn test_i128_boundary() {
		let layout = EncodedRowLayout::new(&[Type::VarInt]);
		let mut row = layout.allocate_row();

		// Value that doesn't fit in 62 bits but fits in i128
		let large = VarInt::from(i64::MAX);
		layout.set_varint(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = layout.get_varint(&row, 0);
		assert_eq!(retrieved, large);

		// Test i128::MAX
		let mut row2 = layout.allocate_row();
		let max_i128 = VarInt::from(i128::MAX);
		layout.set_varint(&mut row2, 0, &max_i128);
		assert_eq!(layout.get_varint(&row2, 0), max_i128);

		// Test i128::MIN
		let mut row3 = layout.allocate_row();
		let min_i128 = VarInt::from(i128::MIN);
		layout.set_varint(&mut row3, 0, &min_i128);
		assert_eq!(layout.get_varint(&row3, 0), min_i128);
	}

	#[test]
	fn test_dynamic_storage() {
		let layout = EncodedRowLayout::new(&[Type::VarInt]);
		let mut row = layout.allocate_row();

		// Create a value larger than i128 can hold
		let huge_str =
			"999999999999999999999999999999999999999999999999";
		let huge = VarInt::from(
			num_bigint::BigInt::parse_bytes(
				huge_str.as_bytes(),
				10,
			)
			.unwrap(),
		);

		layout.set_varint(&mut row, 0, &huge);
		assert!(row.is_defined(0));

		let retrieved = layout.get_varint(&row, 0);
		assert_eq!(retrieved, huge);
		assert_eq!(retrieved.to_string(), huge_str);
	}

	#[test]
	fn test_zero() {
		let layout = EncodedRowLayout::new(&[Type::VarInt]);
		let mut row = layout.allocate_row();

		let zero = VarInt::from(0);
		layout.set_varint(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = layout.get_varint(&row, 0);
		assert_eq!(retrieved, zero);
	}

	#[test]
	fn test_try_get() {
		let layout = EncodedRowLayout::new(&[Type::VarInt]);
		let mut row = layout.allocate_row();

		// Undefined initially
		assert_eq!(layout.try_get_varint(&row, 0), None);

		// Set value
		let value = VarInt::from(12345);
		layout.set_varint(&mut row, 0, &value);
		assert_eq!(layout.try_get_varint(&row, 0), Some(value));
	}

	#[test]
	fn test_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::VarInt]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		let value = VarInt::from(999999999999999i64);
		layout.set_varint(&mut row2, 0, &value);

		assert!(!row1.is_defined(0));
		assert!(row2.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
		assert_eq!(layout.get_varint(&row2, 0), value);
	}

	#[test]
	fn test_multiple_fields() {
		let layout = EncodedRowLayout::new(&[
			Type::Int4,
			Type::VarInt,
			Type::Utf8,
			Type::VarInt,
		]);
		let mut row = layout.allocate_row();

		layout.set_i32(&mut row, 0, 42);

		let small = VarInt::from(100);
		layout.set_varint(&mut row, 1, &small);

		layout.set_utf8(&mut row, 2, "test");

		let large = VarInt::from(i128::MAX);
		layout.set_varint(&mut row, 3, &large);

		assert_eq!(layout.get_i32(&row, 0), 42);
		assert_eq!(layout.get_varint(&row, 1), small);
		assert_eq!(layout.get_utf8(&row, 2), "test");
		assert_eq!(layout.get_varint(&row, 3), large);
	}

	#[test]
	fn test_negative_values() {
		let layout = EncodedRowLayout::new(&[Type::VarInt]);

		// Small negative (i64 inline)
		let mut row1 = layout.allocate_row();
		let small_neg = VarInt::from(-42);
		layout.set_varint(&mut row1, 0, &small_neg);
		assert_eq!(layout.get_varint(&row1, 0), small_neg);

		// Large negative (i128 overflow)
		let mut row2 = layout.allocate_row();
		let large_neg = VarInt::from(i64::MIN);
		layout.set_varint(&mut row2, 0, &large_neg);
		assert_eq!(layout.get_varint(&row2, 0), large_neg);

		// Huge negative (dynamic)
		let mut row3 = layout.allocate_row();
		let huge_neg_str =
			"-999999999999999999999999999999999999999999999999";
		let huge_neg = VarInt::from(
			-num_bigint::BigInt::parse_bytes(
				huge_neg_str.trim_start_matches('-').as_bytes(),
				10,
			)
			.unwrap(),
		);
		layout.set_varint(&mut row3, 0, &huge_neg);
		assert_eq!(layout.get_varint(&row3, 0), huge_neg);
	}
}
