// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_select::filter::FilterPredicate;

use crate::{
	Result,
	util::{bitmap, kernel},
	value::{Value, value_type::ValueType},
};

pub fn get_value(array: &BooleanArray, index: usize) -> Value {
	if index < array.len() {
		Value::Boolean(array.value(index))
	} else {
		Value::none_of(ValueType::Boolean)
	}
}

pub fn as_string(array: &BooleanArray, index: usize) -> String {
	if index < array.len() {
		array.value(index).to_string()
	} else {
		"none".to_string()
	}
}

pub fn slice(array: &BooleanArray, start: usize, end: usize) -> BooleanArray {
	BooleanArray::new(
		bitmap::slice(array.values(), start, end),
		bitmap::slice_nulls(array.logical_nulls().as_ref(), start, end),
	)
}

pub fn take(array: &BooleanArray, num: usize) -> BooleanArray {
	slice(array, 0, num)
}

pub fn filter(array: &BooleanArray, mask: &BooleanBuffer) -> BooleanArray {
	filter_with(array, &kernel::predicate(mask, array.len()))
}

pub fn filter_with(array: &BooleanArray, predicate: &FilterPredicate) -> BooleanArray {
	let selected = kernel::filtered(array, predicate);
	let nulls = kernel::kept_nulls(array, &selected);
	BooleanArray::new(selected.into_parts().0, nulls)
}

pub fn reorder(array: &BooleanArray, indices: &[usize]) -> Result<BooleanArray> {
	kernel::rows_in_range(indices, array.len())?;
	Ok(BooleanArray::new(
		bitmap::reorder(array.values(), indices),
		bitmap::reorder_nulls(array.logical_nulls().as_ref(), indices),
	))
}

pub fn attach_nulls(array: BooleanArray, nulls: Option<NullBuffer>) -> BooleanArray {
	bitmap::assert_nulls_len(nulls.as_ref(), array.len());
	let (values, _) = array.into_parts();
	BooleanArray::new(values, nulls)
}

pub fn capacity(array: &BooleanArray) -> usize {
	let buffer = array.values().inner();
	if buffer.strong_count() == 1 {
		buffer.capacity() * 8
	} else {
		array.len()
	}
}

pub fn heap_size(array: &BooleanArray) -> usize {
	capacity(array).div_ceil(8)
}

#[cfg(test)]
mod tests {
	use arrow_array::BooleanArray;

	use super::{as_string, filter, get_value, reorder, slice, take};
	use crate::value::{Value, value_type::ValueType};

	fn values(array: &BooleanArray) -> Vec<bool> {
		array.values().iter().collect()
	}

	#[test]
	fn get_value_and_as_string_past_the_end_read_as_none() {
		// A row past the end must read as a typed none, never panic.
		let array = BooleanArray::from(vec![true, false]);
		assert_eq!(get_value(&array, 1), Value::Boolean(false));
		assert_eq!(get_value(&array, 2), Value::none_of(ValueType::Boolean));
		assert_eq!(as_string(&array, 0), "true");
		assert_eq!(as_string(&array, 2), "none");
	}

	#[test]
	fn slice_and_take_clamp_like_the_old_container() {
		// Arrow's slice(offset, len) panics past the end; the column slice must clamp instead.
		let array = BooleanArray::from(vec![true, false, true, false]);
		assert_eq!(values(&slice(&array, 1, 3)), vec![false, true]);
		assert_eq!(values(&slice(&array, 2, 40)), vec![true, false]);
		assert_eq!(slice(&array, 3, 1).len(), 0);
		assert_eq!(take(&array, 9).len(), 4);
	}

	#[test]
	fn filter_ignores_a_long_mask_and_reorder_refuses_an_out_of_range_row() {
		// A long mask must not read past len, and an out of range index must fail, never read as false.
		let array = BooleanArray::from(vec![true, false, true]);
		let mask = BooleanArray::from(vec![true, false, true, true, true]);
		assert_eq!(values(&filter(&array, mask.values())), vec![true, true]);
		let error = reorder(&array, &[2, 7, 1]).unwrap_err();
		assert_eq!(error.diagnostic().message, "row index 7 out of range for a column of 3 rows");
	}
}
