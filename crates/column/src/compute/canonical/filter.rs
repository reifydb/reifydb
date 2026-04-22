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
	mask::RowMask,
	nones::NoneBitmap,
};

pub fn filter(array: &CanonicalArray, mask: &RowMask) -> Result<CanonicalArray> {
	assert_eq!(array.len(), mask.len(), "filter: array len {} vs mask len {}", array.len(), mask.len());
	let kept = mask.popcount();

	let new_nones = array.nones.as_ref().map(|n| filter_nones(n, mask, kept));

	let new_storage = match &array.storage {
		CanonicalStorage::Bool(b) => CanonicalStorage::Bool(filter_bool(b, mask, kept)),
		CanonicalStorage::Fixed(f) => {
			CanonicalStorage::Fixed(FixedArray::new(f.ty.clone(), filter_fixed(&f.storage, mask, kept)))
		}
		CanonicalStorage::VarLen(v) => CanonicalStorage::VarLen(filter_varlen(v, mask, kept)),
		CanonicalStorage::BigNum(b) => CanonicalStorage::BigNum(filter_bignum(b, mask, kept)),
	};

	Ok(CanonicalArray::new(array.ty.clone(), array.nullable, new_nones, new_storage))
}

fn filter_nones(nones: &NoneBitmap, mask: &RowMask, kept: usize) -> NoneBitmap {
	let mut out = NoneBitmap::all_present(kept);
	let mut j = 0;
	for i in 0..nones.len() {
		if mask.get(i) {
			if nones.is_none(i) {
				out.set_none(j);
			}
			j += 1;
		}
	}
	out
}

fn filter_bool(b: &BoolArray, mask: &RowMask, kept: usize) -> BoolArray {
	let mut out = BoolArray::new(kept);
	let mut j = 0;
	for i in 0..b.len() {
		if mask.get(i) {
			out.set(j, b.get(i));
			j += 1;
		}
	}
	out
}

fn filter_varlen(v: &VarLenArray, mask: &RowMask, _kept: usize) -> VarLenArray {
	let mut out = VarLenArray::new(v.ty.clone());
	for i in 0..v.len() {
		if mask.get(i) {
			out.push_bytes(v.bytes_at(i));
		}
	}
	out
}

fn filter_bignum(b: &BigNumArray, mask: &RowMask, kept: usize) -> BigNumArray {
	let mut values = Vec::with_capacity(kept);
	for i in 0..b.values.len() {
		if mask.get(i) {
			values.push(b.values[i].clone());
		}
	}
	BigNumArray::from_values(b.ty.clone(), values)
}

fn filter_fixed(storage: &FixedStorage, mask: &RowMask, kept: usize) -> FixedStorage {
	macro_rules! branch {
		($variant:ident, $v:expr) => {{
			let mut out = Vec::with_capacity(kept);
			for i in 0..$v.len() {
				if mask.get(i) {
					out.push($v[i]);
				}
			}
			FixedStorage::$variant(out)
		}};
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
	fn filter_keeps_selected_int4_rows() {
		let cd = ColumnData::int4([10i32, 20, 30, 40, 50]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let mut mask = RowMask::none_set(5);
		mask.set(1, true);
		mask.set(3, true);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 2);
		let CanonicalStorage::Fixed(f) = &out.storage else {
			panic!("expected fixed");
		};
		assert_eq!(f.try_as_slice::<i32>().unwrap(), &[20, 40]);
	}

	#[test]
	fn filter_preserves_none_bitmap_alignment() {
		let mut cd = ColumnData::int4_with_capacity(5);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(50);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let mut mask = RowMask::none_set(5);
		mask.set(0, true);
		mask.set(1, true);
		mask.set(3, true);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.nullable);
		let nones = out.nones.as_ref().unwrap();
		assert!(!nones.is_none(0));
		assert!(nones.is_none(1));
		assert!(nones.is_none(2));
	}
}
