// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{buffer::ColumnBuffer, data::canonical::Canonical, nones::NoneBitmap};
use reifydb_type::{Result, value::Value};

use crate::error::ColumnError;

pub fn take(array: &Canonical, indices: &Canonical) -> Result<Canonical> {
	let idx = extract_indices(indices)?;

	let new_nones = array.nones.as_ref().map(|n| take_nones(n, &idx));
	let new_buffer = array.buffer.gather(&idx);

	Ok(Canonical::new(array.ty.clone(), array.nullable, new_nones, new_buffer))
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

fn extract_indices(indices: &Canonical) -> Result<Vec<usize>> {
	match &indices.buffer {
		ColumnBuffer::Uint1(_)
		| ColumnBuffer::Uint2(_)
		| ColumnBuffer::Uint4(_)
		| ColumnBuffer::Uint8(_)
		| ColumnBuffer::Int4(_)
		| ColumnBuffer::Int8(_) => {
			let len = indices.len();
			let mut out = Vec::with_capacity(len);
			for i in 0..len {
				let v = indices.buffer.get_value(i);
				let n: usize = match v {
					Value::Uint1(n) => n as usize,
					Value::Uint2(n) => n as usize,
					Value::Uint4(n) => n as usize,
					Value::Uint8(n) => n as usize,
					Value::Int4(n) => n as usize,
					Value::Int8(n) => n as usize,
					_ => return Err(ColumnError::TakeIndicesWrongWidth.into()),
				};
				out.push(n);
			}
			Ok(out)
		}
		_ => Err(ColumnError::TakeIndicesNotFixed.into()),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::buffer::ColumnBuffer;

	use super::*;

	#[test]
	fn take_gathers_rows_by_index() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let idx_cd = ColumnBuffer::uint4([4u32, 0, 2]);
		let idx = Canonical::from_column_buffer(&idx_cd).unwrap();
		let out = take(&ca, &idx).unwrap();
		assert_eq!(out.buffer.as_slice::<i32>(), &[50, 10, 30]);
	}
}
