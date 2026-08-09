// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ptr;

use num_bigint::{BigInt, BigUint};
use num_traits::ToPrimitive;
use reifydb_value::{
	reifydb_assertions,
	value::{uint::Uint, value_type::ValueType},
};

use crate::row::{bytes::RowBuilder, shape::RowShape};

const MODE_INLINE: u128 = 0x00000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

const INLINE_VALUE_MASK: u128 = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;

const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

impl RowShape {
	pub fn set_uint(&self, row: &mut impl RowBuilder, index: usize, value: &Uint) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Uint);
		}

		let unsigned_value = value.0.to_biguint().unwrap_or(BigUint::from(0u32));

		if let Some(u128_val) = unsigned_value.to_u128()
			&& u128_val < (1u128 << 127)
		{
			self.remove_dynamic_data(row, index);

			let packed = MODE_INLINE | (u128_val & INLINE_VALUE_MASK);
			// SAFETY: row.len() >= total_static_size() puts the 16-byte slot at field.offset inside the
			// uniquely-owned buffer, and write_unaligned needs no alignment.
			unsafe {
				ptr::write_unaligned(
					row.as_mut_slice().as_mut_ptr().add(field.offset as usize) as *mut u128,
					packed.to_le(),
				);
			}
			self.set_valid(row, index, true);
			return;
		}

		let bytes = unsigned_value.to_bytes_le();
		self.replace_dynamic_data(row, index, &bytes);
	}

	pub fn get_uint(&self, row: &[u8], index: usize) -> Uint {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Uint);
		}

		// SAFETY: row.len() >= total_static_size() puts the 16-byte slot at field.offset inside the row,
		// read_unaligned needs no alignment, and u128 has no invalid bit patterns.
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
			let data_bytes = &row[dynamic_start + offset..dynamic_start + offset + length];

			let unsigned = BigUint::from_bytes_le(data_bytes);
			Uint::from(BigInt::from(unsigned))
		}
	}

	pub fn try_get_uint(&self, row: &[u8], index: usize) -> Option<Uint> {
		if self.is_defined(row, index) && self.fields()[index].constraint.get_type() == ValueType::Uint {
			Some(self.get_uint(row, index))
		} else {
			None
		}
	}
}
