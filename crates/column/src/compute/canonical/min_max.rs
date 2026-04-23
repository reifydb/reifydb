// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{array::canonical::Canonical, buffer::ColumnBuffer};
use reifydb_type::{Result, value::Value};

use crate::error::ColumnError;

pub fn min_max(array: &Canonical) -> Result<(Value, Value)> {
	if array.is_empty() {
		return Err(ColumnError::MinMaxEmpty.into());
	}

	let skip = |row: usize| -> bool { array.nones.as_ref().map(|n| n.is_none(row)).unwrap_or(false) };

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
		ColumnBuffer::Uint16(_) => reduce_int!(array.buffer.as_slice::<u128>(), Uint16),
		ColumnBuffer::Float4(_) | ColumnBuffer::Float8(_) => Err(ColumnError::MinMaxFloatUnsupported.into()),
		_ => Err(ColumnError::FixedArrayRequired {
			operation: "min_max",
		}
		.into()),
	}
}

#[cfg(test)]
mod tests {
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
	fn min_max_skips_nones() {
		let mut cd = ColumnBuffer::int4_with_capacity(5);
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(50);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int4(10));
		assert_eq!(max, Value::Int4(50));
	}
}
