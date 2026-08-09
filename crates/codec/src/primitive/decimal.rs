// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bigdecimal::BigDecimal as StdBigDecimal;
use num_bigint::BigInt as StdBigInt;
use reifydb_value::{
	reifydb_assertions,
	value::{decimal::Decimal, value_type::ValueType},
};

use crate::row::{bytes::RowBuilder, shape::RowShape};

#[cfg(reifydb_assertions)]
const MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
#[cfg(reifydb_assertions)]
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

impl RowShape {
	pub fn set_decimal(&self, row: &mut impl RowBuilder, index: usize, value: &Decimal) {
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), ValueType::Decimal);
		}

		let (mantissa, original_scale) = value.inner().as_bigint_and_exponent();
		let scale_bytes = original_scale.to_le_bytes();
		let digits_bytes = mantissa.to_signed_bytes_le();

		let mut serialized = Vec::with_capacity(8 + digits_bytes.len());
		serialized.extend_from_slice(&scale_bytes);
		serialized.extend_from_slice(&digits_bytes);

		self.replace_dynamic_data(row, index, &serialized);
	}

	pub fn get_decimal(&self, row: &[u8], index: usize) -> Decimal {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Decimal);
		}

		// SAFETY: row.len() >= total_static_size() puts the 16-byte slot at field.offset inside the row,
		// read_unaligned needs no alignment, and u128 has no invalid bit patterns.
		let packed = unsafe { (row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned() };
		let packed = u128::from_le(packed);

		reifydb_assertions! {
			assert!(packed & MODE_MASK == MODE_DYNAMIC, "Expected dynamic storage");
		}

		let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
		let length = ((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

		let dynamic_start = self.dynamic_section_start();
		let data_bytes = &row[dynamic_start + offset..dynamic_start + offset + length];

		let original_scale = i64::from_le_bytes(data_bytes[0..8].try_into().unwrap());
		let mantissa = StdBigInt::from_signed_bytes_le(&data_bytes[8..]);

		let big_decimal = StdBigDecimal::new(mantissa, original_scale);

		Decimal::from(big_decimal)
	}

	pub fn try_get_decimal(&self, row: &[u8], index: usize) -> Option<Decimal> {
		if self.is_defined(row, index)
			&& matches!(self.fields()[index].constraint.get_type().inner_type(), ValueType::Decimal)
		{
			Some(self.get_decimal(row, index))
		} else {
			None
		}
	}
}
