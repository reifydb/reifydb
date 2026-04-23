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
	error::ColumnError,
	nones::NoneBitmap,
};

pub fn take(array: &CanonicalArray, indices: &CanonicalArray) -> Result<CanonicalArray> {
	let idx = extract_indices(indices)?;

	let new_nones = array.nones.as_ref().map(|n| take_nones(n, &idx));

	let new_storage = match &array.storage {
		CanonicalStorage::Bool(b) => {
			let mut out = BoolArray::new(idx.len());
			for (j, &i) in idx.iter().enumerate() {
				out.set(j, b.get(i));
			}
			CanonicalStorage::Bool(out)
		}
		CanonicalStorage::Fixed(f) => {
			CanonicalStorage::Fixed(FixedArray::new(f.ty.clone(), take_fixed(&f.storage, &idx)))
		}
		CanonicalStorage::VarLen(v) => {
			let mut out = VarLenArray::new(v.ty.clone());
			for &i in &idx {
				out.push_bytes(v.bytes_at(i));
			}
			CanonicalStorage::VarLen(out)
		}
		CanonicalStorage::BigNum(b) => {
			let values: Vec<_> = idx.iter().map(|&i| b.values[i].clone()).collect();
			CanonicalStorage::BigNum(BigNumArray::from_values(b.ty.clone(), values))
		}
	};

	Ok(CanonicalArray::new(array.ty.clone(), array.nullable, new_nones, new_storage))
}

fn take_nones(nones: &NoneBitmap, idx: &[usize]) -> NoneBitmap {
	let mut out = NoneBitmap::all_present(idx.len());
	for (j, &i) in idx.iter().enumerate() {
		if nones.is_none(i) {
			out.set_none(j);
		}
	}
	out
}

fn extract_indices(indices: &CanonicalArray) -> Result<Vec<usize>> {
	let CanonicalStorage::Fixed(f) = &indices.storage else {
		return Err(ColumnError::TakeIndicesNotFixed.into());
	};
	Ok(match &f.storage {
		FixedStorage::U8(v) => v.iter().map(|&i| i as usize).collect(),
		FixedStorage::U16(v) => v.iter().map(|&i| i as usize).collect(),
		FixedStorage::U32(v) => v.iter().map(|&i| i as usize).collect(),
		FixedStorage::U64(v) => v.iter().map(|&i| i as usize).collect(),
		FixedStorage::I32(v) => v.iter().map(|&i| i as usize).collect(),
		FixedStorage::I64(v) => v.iter().map(|&i| i as usize).collect(),
		_ => return Err(ColumnError::TakeIndicesWrongWidth.into()),
	})
}

fn take_fixed(storage: &FixedStorage, idx: &[usize]) -> FixedStorage {
	macro_rules! branch {
		($variant:ident, $v:expr) => {
			FixedStorage::$variant(idx.iter().map(|&i| $v[i]).collect())
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
	fn take_gathers_rows_by_index() {
		let cd = ColumnData::int4([10i32, 20, 30, 40, 50]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let idx_cd = ColumnData::uint4([4u32, 0, 2]);
		let idx = CanonicalArray::from_column_data(&idx_cd).unwrap();
		let out = take(&ca, &idx).unwrap();
		let CanonicalStorage::Fixed(f) = &out.storage else {
			panic!("expected fixed");
		};
		assert_eq!(f.try_as_slice::<i32>().unwrap(), &[50, 10, 30]);
	}
}
