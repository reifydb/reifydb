// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{array::canonical::Canonical, mask::RowMask, nones::NoneBitmap};
use reifydb_type::{Result, util::bitvec::BitVec};

pub fn filter(array: &Canonical, mask: &RowMask) -> Result<Canonical> {
	assert_eq!(array.len(), mask.len(), "filter: array len {} vs mask len {}", array.len(), mask.len());
	let kept = mask.popcount();

	let new_nones = array.nones.as_ref().map(|n| filter_nones(n, mask, kept));

	let mut new_buffer = array.buffer.clone();
	new_buffer.filter(&row_mask_to_bitvec(mask))?;

	Ok(Canonical::new(array.ty.clone(), array.nullable, new_nones, new_buffer))
}

fn row_mask_to_bitvec(mask: &RowMask) -> BitVec {
	let mut bits = Vec::with_capacity(mask.len());
	for i in 0..mask.len() {
		bits.push(mask.get(i));
	}
	BitVec::from(bits)
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

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::buffer::ColumnBuffer;

	use super::*;

	#[test]
	fn filter_keeps_selected_int4_rows() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let mut mask = RowMask::none_set(5);
		mask.set(1, true);
		mask.set(3, true);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 2);
		assert_eq!(out.buffer.as_slice::<i32>(), &[20, 40]);
	}

	#[test]
	fn filter_preserves_none_bitmap_alignment() {
		let mut cd = ColumnBuffer::int4_with_capacity(5);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(50);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
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
