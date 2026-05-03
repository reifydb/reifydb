// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{array::canonical::Canonical, buffer::ColumnBuffer};
use reifydb_type::{Result, value::Value};

use crate::error::ColumnError;

pub fn sum(array: &Canonical) -> Result<Value> {
	let skip = |row: usize| -> bool { array.nones.as_ref().map(|n| n.is_none(row)).unwrap_or(false) };

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
		ColumnBuffer::Uint16(_) => sum_int_slice!(array.buffer.as_slice::<u128>(), u128, Uint16),
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
	use super::*;

	#[test]
	fn sum_int4_widens_to_int8() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int8(100));
	}

	#[test]
	fn sum_skips_nones() {
		let mut cd = ColumnBuffer::int4_with_capacity(4);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int8(40));
	}
}
