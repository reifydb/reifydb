// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ptr;

use num_bigint::BigInt as StdBigInt;
use num_traits::ToPrimitive;
use reifydb_value::{
	reifydb_assertions,
	value::{int::Int, value_type::ValueType},
};

use crate::row::{
	bytes::{EncodedRowBuilder, read_defined},
	shape::RowShape,
};

const MODE_INLINE: u128 = 0x00000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

const INLINE_VALUE_MASK: u128 = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;

const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

impl RowShape {
	pub fn set_int(&self, row: &mut EncodedRowBuilder, index: usize, value: &Int) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Int);
		}

		if let Some(i128_val) = value.0.to_i128()
			&& (-(1i128 << 126)..(1i128 << 126)).contains(&i128_val)
		{
			self.remove_dynamic_data(row, index);

			let packed = MODE_INLINE | ((i128_val as u128) & INLINE_VALUE_MASK);
			// SAFETY: row.len() >= total_static_size() puts the 16-byte slot at field.offset inside the
			// uniquely-owned buffer, and write_unaligned needs no alignment.
			unsafe {
				ptr::write_unaligned(
					row.as_mut_slice().as_mut_ptr().add(field.offset as usize) as *mut u128,
					packed.to_le(),
				);
			}
			row.set_valid(index, true);
			return;
		}

		let bytes = value.0.to_signed_bytes_le();
		self.replace_dynamic_data(row, index, &bytes);
	}

	pub fn get_int(&self, row: &[u8], index: usize) -> Int {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Int);
		}

		// SAFETY: row.len() >= total_static_size() puts the 16-byte slot at field.offset inside the row,
		// read_unaligned needs no alignment, and u128 has no invalid bit patterns.
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
			let bigint_bytes = &row[dynamic_start + offset..dynamic_start + offset + length];

			Int::from(StdBigInt::from_signed_bytes_le(bigint_bytes))
		}
	}

	pub fn try_get_int(&self, row: &[u8], index: usize) -> Option<Int> {
		if read_defined(row, index) && self.fields()[index].constraint.get_type() == ValueType::Int {
			Some(self.get_int(row, index))
		} else {
			None
		}
	}
}
