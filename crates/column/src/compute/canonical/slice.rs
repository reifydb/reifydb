// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use crate::{
	array::{
		bignum::BigNumArray,
		boolean::BoolArray,
		canonical::{CanonicalArray, CanonicalStorage},
		fixed::{FixedArray, FixedStorage},
		varlen::VarLenArray,
	},
	nones::NoneBitmap,
};

pub fn slice(array: &CanonicalArray, start: usize, end: usize) -> Result<CanonicalArray> {
	assert!(start <= end);
	assert!(end <= array.len());
	let count = end - start;

	let new_nones = array.nones.as_ref().map(|n| slice_nones(n, start, end));

	let new_storage = match &array.storage {
		CanonicalStorage::Bool(b) => {
			let mut out = BoolArray::new(count);
			for i in 0..count {
				out.set(i, b.get(start + i));
			}
			CanonicalStorage::Bool(out)
		}
		CanonicalStorage::Fixed(f) => {
			CanonicalStorage::Fixed(FixedArray::new(f.ty.clone(), slice_fixed(&f.storage, start, end)))
		}
		CanonicalStorage::VarLen(v) => {
			let mut out = VarLenArray::new(v.ty.clone());
			for i in start..end {
				out.push_bytes(v.bytes_at(i));
			}
			CanonicalStorage::VarLen(out)
		}
		CanonicalStorage::BigNum(b) => {
			CanonicalStorage::BigNum(BigNumArray::from_values(b.ty.clone(), b.values[start..end].to_vec()))
		}
	};

	Ok(CanonicalArray::new(array.ty.clone(), array.nullable, new_nones, new_storage))
}

fn slice_nones(nones: &NoneBitmap, start: usize, end: usize) -> NoneBitmap {
	let count = end - start;
	let mut out = NoneBitmap::all_present(count);
	for i in 0..count {
		if nones.is_none(start + i) {
			out.set_none(i);
		}
	}
	out
}

fn slice_fixed(storage: &FixedStorage, start: usize, end: usize) -> FixedStorage {
	macro_rules! branch {
		($variant:ident, $v:expr) => {
			FixedStorage::$variant($v[start..end].to_vec())
		};
	}
	match storage {
		FixedStorage::I8(v) => branch!(I8, v),
		FixedStorage::I16(v) => branch!(I16, v),
		FixedStorage::I32(v) => branch!(I32, v),
		FixedStorage::I64(v) => branch!(I64, v),
		FixedStorage::I128(v) => branch!(I128, v),
		FixedStorage::U8(v) => branch!(U8, v),
		FixedStorage::U16(v) => branch!(U16, v),
		FixedStorage::U32(v) => branch!(U32, v),
		FixedStorage::U64(v) => branch!(U64, v),
		FixedStorage::U128(v) => branch!(U128, v),
		FixedStorage::F32(v) => branch!(F32, v),
		FixedStorage::F64(v) => branch!(F64, v),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::data::ColumnData;

	use super::*;

	#[test]
	fn slice_fixed_returns_subrange() {
		let cd = ColumnData::int4([10i32, 20, 30, 40, 50]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = slice(&ca, 1, 4).unwrap();
		let CanonicalStorage::Fixed(f) = &out.storage else {
			panic!("expected fixed");
		};
		assert_eq!(f.try_as_slice::<i32>().unwrap(), &[20, 30, 40]);
	}

	#[test]
	fn slice_preserves_nullability_and_bitmap() {
		let mut cd = ColumnData::int4_with_capacity(4);
		cd.push_none();
		cd.push::<i32>(20);
		cd.push_none();
		cd.push::<i32>(40);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = slice(&ca, 1, 4).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.nullable);
		let nones = out.nones.as_ref().unwrap();
		assert!(!nones.is_none(0));
		assert!(nones.is_none(1));
		assert!(!nones.is_none(2));
	}
}
