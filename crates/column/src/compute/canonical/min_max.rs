// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{buffer::ColumnBuffer, data::canonical::Canonical};
use reifydb_value::{
	Result,
	value::{Value, container::decimal_array::u128s},
};

use crate::error::ColumnError;

pub fn min_max(array: &Canonical) -> Result<(Value, Value)> {
	if array.is_empty() {
		return Err(ColumnError::MinMaxEmpty.into());
	}

	let skip = |row: usize| -> bool { array.nones.as_ref().map(|n| n.is_null(row)).unwrap_or(false) };

	macro_rules! reduce_int {
		($slice:expr, $variant:ident) => {{
			let mut min = None;
			let mut max = None;
			for (i, &x) in $slice.iter().enumerate() {
				if skip(i) {
					continue;
				}
				min = Some(min.map_or(x, |m: _| {
					if x < m {
						x
					} else {
						m
					}
				}));
				max = Some(max.map_or(x, |m: _| {
					if x > m {
						x
					} else {
						m
					}
				}));
			}
			match (min, max) {
				(Some(min), Some(max)) => Ok((Value::$variant(min), Value::$variant(max))),
				_ => Err(ColumnError::MinMaxAllNone.into()),
			}
		}};
	}

	match &array.buffer {
		ColumnBuffer::Int1(_) => reduce_int!(array.buffer.as_slice::<i8>(), Int1),
		ColumnBuffer::Int2(_) => reduce_int!(array.buffer.as_slice::<i16>(), Int2),
		ColumnBuffer::Int4(_) => reduce_int!(array.buffer.as_slice::<i32>(), Int4),
		ColumnBuffer::Int8(_) => reduce_int!(array.buffer.as_slice::<i64>(), Int8),
		ColumnBuffer::Int16(_) => reduce_int!(array.buffer.as_slice::<i128>(), Int16),
		ColumnBuffer::Uint1(_) => reduce_int!(array.buffer.as_slice::<u8>(), Uint1),
		ColumnBuffer::Uint2(_) => reduce_int!(array.buffer.as_slice::<u16>(), Uint2),
		ColumnBuffer::Uint4(_) => reduce_int!(array.buffer.as_slice::<u32>(), Uint4),
		ColumnBuffer::Uint8(_) => reduce_int!(array.buffer.as_slice::<u64>(), Uint8),
		ColumnBuffer::Uint16(c) => reduce_int!(u128s(c), Uint16),
		ColumnBuffer::Any(_) => Err(ColumnError::FixedArrayRequired {
			operation: "min_max",
		}
		.into()),
		_ => reduce_ordered(array),
	}
}

fn reduce_ordered(array: &Canonical) -> Result<(Value, Value)> {
	let mut min: Option<Value> = None;
	let mut max: Option<Value> = None;
	for row in 0..array.len() {
		if array.nones.as_ref().map(|n| n.is_null(row)).unwrap_or(false) {
			continue;
		}
		let value = array.buffer.get_value(row);
		if min.as_ref().is_none_or(|current| value < *current) {
			min = Some(value.clone());
		}
		if max.as_ref().is_none_or(|current| value > *current) {
			max = Some(value);
		}
	}
	match (min, max) {
		(Some(min), Some(max)) => Ok((min, max)),
		_ => Err(ColumnError::MinMaxAllNone.into()),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::builder::ColumnBuilder;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[test]
	fn min_max_over_int4() {
		let cd = ColumnBuffer::int4([30i32, 10, 50, 20, 40]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int4(10));
		assert_eq!(max, Value::Int4(50));
	}

	#[test]
	fn min_max_uint16_above_u64() {
		// A lossy i256 cast collapses rows above u64 MAX to none or equal values and picks the wrong bounds.
		let cd = ColumnBuffer::uint16([u128::MAX, (1u128 << 64) + 1, 1u128 << 64]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Uint16(1u128 << 64));
		assert_eq!(max, Value::Uint16(u128::MAX));
	}

	#[test]
	fn min_max_int16_at_i128_bounds() {
		// A truncating read of the i128 rows breaks the order at the extremes and picks the wrong bounds.
		let cd = ColumnBuffer::int16([0i128, i128::MAX, -1, i128::MIN, 1]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int16(i128::MIN));
		assert_eq!(max, Value::Int16(i128::MAX));
	}

	#[test]
	fn min_max_skips_nones() {
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 5);
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(50);
		let cd = cd.finish();
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int4(10));
		assert_eq!(max, Value::Int4(50));
	}
}
