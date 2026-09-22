// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer};
use reifydb_core::value::column::data::canonical::Canonical;
use reifydb_value::Result;

pub fn filter(array: &Canonical, mask: &BooleanBuffer) -> Result<Canonical> {
	assert_eq!(array.len(), mask.len(), "filter: array len {} vs mask len {}", array.len(), mask.len());
	let kept = mask.count_set_bits();

	let new_nones = array.nones.as_ref().map(|n| filter_nones(n, mask, kept));

	let mut new_buffer = array.buffer.clone();
	new_buffer.filter(mask)?;

	Ok(Canonical::new(array.ty.clone(), array.nullable, new_nones, new_buffer))
}

fn filter_nones(nones: &NullBuffer, mask: &BooleanBuffer, kept: usize) -> NullBuffer {
	let mut out = BooleanBufferBuilder::new(kept);
	out.append_n(kept, true);
	let mut j = 0;
	for i in 0..nones.len() {
		if mask.value(i) {
			if nones.is_null(i) {
				out.set_bit(j, false);
			}
			j += 1;
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
	fn filter_keeps_selected_int4_rows() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let mask = BooleanBuffer::from(vec![false, true, false, true, false]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 2);
		assert_eq!(out.buffer.as_slice::<i32>(), &[20, 40]);
	}

	#[test]
	fn filter_preserves_none_bitmap_alignment() {
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 5);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(50);
		let cd = cd.finish();
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let mask = BooleanBuffer::from(vec![true, true, false, true, false]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.nullable);
		let nones = out.nones.as_ref().unwrap();
		assert!(!nones.is_null(0));
		assert!(nones.is_null(1));
		assert!(nones.is_null(2));
	}
}
