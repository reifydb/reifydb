// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use reifydb_core::value::column::data::canonical::Canonical;
use reifydb_value::Result;

pub fn slice(array: &Canonical, start: usize, end: usize) -> Result<Canonical> {
	assert!(start <= end);
	assert!(end <= array.len());

	let new_nones = array.nones.as_ref().map(|n| slice_nones(n, start, end));
	let new_buffer = array.buffer.slice(start, end);

	Ok(Canonical::new(array.ty.clone(), array.nullable, new_nones, new_buffer))
}

fn slice_nones(nones: &NullBuffer, start: usize, end: usize) -> NullBuffer {
	let count = end - start;
	let mut out = BooleanBufferBuilder::new(count);
	out.append_n(count, true);
	for i in 0..count {
		if nones.is_null(start + i) {
			out.set_bit(i, false);
		}
	}
	NullBuffer::new(out.finish())
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};
	use reifydb_value::value::value_type::ValueType;

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
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 4);
		cd.push_none();
		cd.push::<i32>(20);
		cd.push_none();
		cd.push::<i32>(40);
		let cd = cd.finish();
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let out = slice(&ca, 1, 4).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.nullable);
		let nones = out.nones.as_ref().unwrap();
		assert!(!nones.is_null(0));
		assert!(nones.is_null(1));
		assert!(!nones.is_null(2));
	}
}
