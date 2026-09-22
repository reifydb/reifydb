// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{buffer::ColumnBuffer, data::canonical::Canonical};
use reifydb_value::{
	Result,
	value::{Value, container::decimal_array::u128s},
};

use crate::error::ColumnError;

pub fn sum(array: &Canonical) -> Result<Value> {
	let skip = |row: usize| -> bool { array.nones.as_ref().map(|n| n.is_null(row)).unwrap_or(false) };

	macro_rules! sum_int_slice {
		($slice:expr, $acc_ty:ty, $variant:ident) => {{
			let mut acc: $acc_ty = 0;
			for (i, &x) in $slice.iter().enumerate() {
				if !skip(i) {
					acc = acc.wrapping_add(x as $acc_ty);
				}
			}
			Value::$variant(acc)
		}};
	}

	let v = match &array.buffer {
		ColumnBuffer::Int1(_) => sum_int_slice!(array.buffer.as_slice::<i8>(), i64, Int8),
		ColumnBuffer::Int2(_) => sum_int_slice!(array.buffer.as_slice::<i16>(), i64, Int8),
		ColumnBuffer::Int4(_) => sum_int_slice!(array.buffer.as_slice::<i32>(), i64, Int8),
		ColumnBuffer::Int8(_) => sum_int_slice!(array.buffer.as_slice::<i64>(), i64, Int8),
		ColumnBuffer::Int16(_) => sum_int_slice!(array.buffer.as_slice::<i128>(), i128, Int16),
		ColumnBuffer::Uint1(_) => sum_int_slice!(array.buffer.as_slice::<u8>(), u64, Uint8),
		ColumnBuffer::Uint2(_) => sum_int_slice!(array.buffer.as_slice::<u16>(), u64, Uint8),
		ColumnBuffer::Uint4(_) => sum_int_slice!(array.buffer.as_slice::<u32>(), u64, Uint8),
		ColumnBuffer::Uint8(_) => sum_int_slice!(array.buffer.as_slice::<u64>(), u64, Uint8),
		ColumnBuffer::Uint16(c) => sum_int_slice!(u128s(c), u128, Uint16),
		ColumnBuffer::Float4(_) => {
			let slice = array.buffer.as_slice::<f32>();
			let mut acc = 0f64;
			for (i, &x) in slice.iter().enumerate() {
				if !skip(i) {
					acc += x as f64;
				}
			}
			Value::float8(acc)
		}
		ColumnBuffer::Float8(_) => {
			let slice = array.buffer.as_slice::<f64>();
			let mut acc = 0f64;
			for (i, &x) in slice.iter().enumerate() {
				if !skip(i) {
					acc += x;
				}
			}
			Value::float8(acc)
		}
		_ => {
			return Err(ColumnError::FixedArrayRequired {
				operation: "sum",
			}
			.into());
		}
	};
	Ok(v)
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::builder::ColumnBuilder;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[test]
	fn sum_int4_widens_to_int8() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int8(100));
	}

	#[test]
	fn sum_uint16_keeps_bits_above_u64() {
		// A lossy i256 cast drops the bits above 64 and sums these rows to 5 or none.
		let cd = ColumnBuffer::uint16([1u128 << 64, (1u128 << 64) + 5]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Uint16((1u128 << 65) + 5));
	}

	#[test]
	fn sum_uint16_at_u128_max_wraps() {
		// The sum must stay u128 and wrap, otherwise u128 MAX plus 2 panics or saturates instead of 1.
		let single = ColumnBuffer::uint16([u128::MAX]);
		let ca = Canonical::from_column_buffer(&single).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Uint16(u128::MAX));

		let wrapping = ColumnBuffer::uint16([u128::MAX, 2]);
		let ca = Canonical::from_column_buffer(&wrapping).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Uint16(1));
	}

	#[test]
	fn sum_int16_at_i128_bounds() {
		// The sum must stay i128 and wrap, otherwise i128 MAX plus 1 panics instead of giving i128 MIN.
		let min = ColumnBuffer::int16([i128::MIN]);
		let ca = Canonical::from_column_buffer(&min).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int16(i128::MIN));

		let max = ColumnBuffer::int16([i128::MAX]);
		let ca = Canonical::from_column_buffer(&max).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int16(i128::MAX));

		let both = ColumnBuffer::int16([i128::MIN, i128::MAX]);
		let ca = Canonical::from_column_buffer(&both).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int16(-1));

		let wrapping = ColumnBuffer::int16([i128::MAX, 1]);
		let ca = Canonical::from_column_buffer(&wrapping).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int16(i128::MIN));
	}

	#[test]
	fn sum_skips_nones() {
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 4);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		let cd = cd.finish();
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int8(40));
	}
}
