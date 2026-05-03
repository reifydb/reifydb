// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{data::canonical::Canonical, nones::NoneBitmap};
use reifydb_type::Result;

pub fn slice(array: &Canonical, start: usize, end: usize) -> Result<Canonical> {
	assert!(start <= end);
	assert!(end <= array.len());

	let new_nones = array.nones.as_ref().map(|n| slice_nones(n, start, end));
	let new_buffer = array.buffer.slice(start, end);

	Ok(Canonical::new(array.ty.clone(), array.nullable, new_nones, new_buffer))
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

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::buffer::ColumnBuffer;

	use super::*;

	#[test]
	fn slice_fixed_returns_subrange() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let out = slice(&ca, 1, 4).unwrap();
		assert_eq!(out.buffer.as_slice::<i32>(), &[20, 30, 40]);
	}

	#[test]
	fn slice_preserves_nullability_and_bitmap() {
		let mut cd = ColumnBuffer::int4_with_capacity(4);
		cd.push_none();
		cd.push::<i32>(20);
		cd.push_none();
		cd.push::<i32>(40);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let out = slice(&ca, 1, 4).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.nullable);
		let nones = out.nones.as_ref().unwrap();
		assert!(!nones.is_none(0));
		assert!(nones.is_none(1));
		assert!(!nones.is_none(2));
	}
}
