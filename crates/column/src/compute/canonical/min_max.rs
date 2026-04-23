// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{Result, value::Value};

use crate::{
	array::{
		canonical::{CanonicalArray, CanonicalStorage},
		fixed::FixedStorage,
	},
	error::ColumnError,
};

pub fn min_max(array: &CanonicalArray) -> Result<(Value, Value)> {
	if array.is_empty() {
		return Err(ColumnError::MinMaxEmpty.into());
	}

	let CanonicalStorage::Fixed(f) = &array.storage else {
		return Err(ColumnError::FixedArrayRequired {
			operation: "min_max",
		}
		.into());
	};

	let skip = |row: usize| -> bool { array.nones.as_ref().map(|n| n.is_none(row)).unwrap_or(false) };

	macro_rules! reduce_int {
		($v:expr, $variant:ident) => {{
			let mut min = None;
			let mut max = None;
			for (i, &x) in $v.iter().enumerate() {
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

	match &f.storage {
		FixedStorage::I8(v) => reduce_int!(v, Int1),
		FixedStorage::I16(v) => reduce_int!(v, Int2),
		FixedStorage::I32(v) => reduce_int!(v, Int4),
		FixedStorage::I64(v) => reduce_int!(v, Int8),
		FixedStorage::I128(v) => reduce_int!(v, Int16),
		FixedStorage::U8(v) => reduce_int!(v, Uint1),
		FixedStorage::U16(v) => reduce_int!(v, Uint2),
		FixedStorage::U32(v) => reduce_int!(v, Uint4),
		FixedStorage::U64(v) => reduce_int!(v, Uint8),
		FixedStorage::U128(v) => reduce_int!(v, Uint16),
		FixedStorage::F32(_) | FixedStorage::F64(_) => Err(ColumnError::MinMaxFloatUnsupported.into()),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::data::ColumnData;

	use super::*;

	#[test]
	fn min_max_over_int4() {
		let cd = ColumnData::int4([30i32, 10, 50, 20, 40]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int4(10));
		assert_eq!(max, Value::Int4(50));
	}

	#[test]
	fn min_max_skips_nones() {
		let mut cd = ColumnData::int4_with_capacity(5);
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(50);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int4(10));
		assert_eq!(max, Value::Int4(50));
	}
}
