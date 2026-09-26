// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_buffer::{MutableBuffer, NullBuffer};
use arrow_select::filter::FilterPredicate;

use crate::util::{bitmap, kernel};

pub fn assert_whole_rows(width: usize, bytes: usize) {
	assert_eq!(
		bytes % width,
		0,
		"fixed size binary buffer of {bytes} bytes is not a whole number of {width} byte rows"
	);
}

pub fn from_buffer(width: usize, buffer: MutableBuffer) -> FixedSizeBinaryArray {
	assert_whole_rows(width, buffer.len());
	FixedSizeBinaryArray::new(width as i32, buffer.into(), None)
}

pub fn slice(array: &FixedSizeBinaryArray, start: usize, end: usize) -> FixedSizeBinaryArray {
	let end = end.min(array.len());
	let start = start.min(end);
	array.slice(start, end - start)
}

pub fn take(array: &FixedSizeBinaryArray, num: usize) -> FixedSizeBinaryArray {
	slice(array, 0, num)
}

pub fn filter_with(array: &FixedSizeBinaryArray, predicate: &FilterPredicate) -> FixedSizeBinaryArray {
	let selected = kernel::filtered(array, predicate);
	let nulls = kernel::kept_nulls(array, &selected);
	attach_nulls(selected, nulls)
}

pub fn reorder(array: &FixedSizeBinaryArray, indices: &[usize], filler: &[u8]) -> FixedSizeBinaryArray {
	let width = array.value_length() as usize;
	assert_eq!(
		filler.len(),
		width,
		"reorder filler of {} bytes does not match the {width} byte rows",
		filler.len()
	);
	let bytes = &array.value_data()[..array.len() * width];
	let mut reordered = MutableBuffer::with_capacity(indices.len() * width);
	for &idx in indices {
		if idx < array.len() {
			reordered.extend_from_slice(&bytes[idx * width..(idx + 1) * width]);
		} else {
			reordered.extend_from_slice(filler);
		}
	}
	attach_nulls(from_buffer(width, reordered), bitmap::reorder_nulls(array.nulls(), indices))
}

pub fn attach_nulls(array: FixedSizeBinaryArray, nulls: Option<NullBuffer>) -> FixedSizeBinaryArray {
	bitmap::assert_nulls_len(nulls.as_ref(), array.len());
	let (width, values, _) = array.into_parts();
	FixedSizeBinaryArray::new(width, values, nulls)
}

pub fn capacity(array: &FixedSizeBinaryArray) -> usize {
	let buffer = array.values();
	if buffer.strong_count() == 1 {
		buffer.capacity() / array.value_length() as usize
	} else {
		array.len()
	}
}

pub fn heap_size(array: &FixedSizeBinaryArray) -> usize {
	capacity(array) * array.value_length() as usize
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	#[should_panic(expected = "reorder filler of 3 bytes")]
	fn reorder_rejects_filler_of_wrong_width() {
		// Otherwise a short filler shifts every later row out of its 16 byte slot.
		let array = from_buffer(16, MutableBuffer::from_len_zeroed(16));
		reorder(&array, &[0, 1], &[0; 3]);
	}
}
