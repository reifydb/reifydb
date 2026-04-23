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

// Sum of all non-None rows. Overflow semantics follow Rust's wrapping behavior
// in release builds; debug builds panic on overflow. v1 restricts to fixed-
// width integer and float columns; BigNum/VarLen/Bool return an error.
pub fn sum(array: &CanonicalArray) -> Result<Value> {
	let CanonicalStorage::Fixed(f) = &array.storage else {
		return Err(ColumnError::FixedArrayRequired {
			operation: "sum",
		}
		.into());
	};

	let skip = |row: usize| -> bool { array.nones.as_ref().map(|n| n.is_none(row)).unwrap_or(false) };

	macro_rules! sum_int {
		($v:expr, $acc_ty:ty, $variant:ident) => {{
			let mut acc: $acc_ty = 0;
			for (i, &x) in $v.iter().enumerate() {
				if !skip(i) {
					acc = acc.wrapping_add(x as $acc_ty);
				}
			}
			Ok(Value::$variant(acc))
		}};
	}

	match &f.storage {
		// Widen narrow ints to i64/u64 to reduce (but not eliminate) overflow risk.
		FixedStorage::I8(v) => sum_int!(v, i64, Int8),
		FixedStorage::I16(v) => sum_int!(v, i64, Int8),
		FixedStorage::I32(v) => sum_int!(v, i64, Int8),
		FixedStorage::I64(v) => sum_int!(v, i64, Int8),
		FixedStorage::I128(v) => sum_int!(v, i128, Int16),
		FixedStorage::U8(v) => sum_int!(v, u64, Uint8),
		FixedStorage::U16(v) => sum_int!(v, u64, Uint8),
		FixedStorage::U32(v) => sum_int!(v, u64, Uint8),
		FixedStorage::U64(v) => sum_int!(v, u64, Uint8),
		FixedStorage::U128(v) => sum_int!(v, u128, Uint16),
		FixedStorage::F32(v) => {
			let mut acc = 0f64;
			for (i, &x) in v.iter().enumerate() {
				if !skip(i) {
					acc += x as f64;
				}
			}
			Ok(Value::float8(acc))
		}
		FixedStorage::F64(v) => {
			let mut acc = 0f64;
			for (i, &x) in v.iter().enumerate() {
				if !skip(i) {
					acc += x;
				}
			}
			Ok(Value::float8(acc))
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::data::ColumnData;

	use super::*;

	#[test]
	fn sum_int4_widens_to_int8() {
		let cd = ColumnData::int4([10i32, 20, 30, 40]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int8(100));
	}

	#[test]
	fn sum_skips_nones() {
		let mut cd = ColumnData::int4_with_capacity(4);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(sum(&ca).unwrap(), Value::Int8(40));
	}
}
