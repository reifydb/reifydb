// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	util::bitmap,
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
	BooleanArray::from(bitmap::slice(array.values(), start, end))
}

pub fn take(array: &BooleanArray, num: usize) -> BooleanArray {
	slice(array, 0, num)
}

pub fn filter(array: &BooleanArray, mask: &BooleanBuffer) -> BooleanArray {
	BooleanArray::from(bitmap::filter(array.values(), mask))
}

pub fn reorder(array: &BooleanArray, indices: &[usize]) -> BooleanArray {
	BooleanArray::from(bitmap::reorder(array.values(), indices))
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

pub fn serialize<Ser: Serializer>(array: &BooleanArray, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	#[derive(Serialize)]
	struct Helper<'a> {
		#[serde(serialize_with = "bitmap::serialize")]
		data: &'a BooleanBuffer,
	}
	Helper {
		data: array.values(),
	}
	.serialize(serializer)
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<BooleanArray, D::Error> {
	#[derive(Deserialize)]
	struct Helper {
		#[serde(deserialize_with = "bitmap::deserialize")]
		data: BooleanBuffer,
	}
	Ok(BooleanArray::from(Helper::deserialize(deserializer)?.data))
}

#[cfg(test)]
mod tests {
	use arrow_array::BooleanArray;
	use serde::{Deserialize, Serialize};
	use serde_json::{from_str, to_string};

	use super::{as_string, filter, get_value, reorder, slice, take};
	use crate::value::{Value, value_type::ValueType};

	#[derive(Serialize, Deserialize)]
	struct Wrap(#[serde(with = "super")] BooleanArray);

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
	fn filter_and_reorder_clamp_out_of_range_rows() {
		// A long mask must not read past len, and an out of range index must give false.
		let array = BooleanArray::from(vec![true, false, true]);
		let mask = BooleanArray::from(vec![true, false, true, true, true]);
		assert_eq!(values(&filter(&array, mask.values())), vec![true, true]);
		assert_eq!(values(&reorder(&array, &[2, 7, 1])), vec![true, false, false]);
	}

	#[test]
	fn serde_keeps_the_bool_container_shape() {
		// Stored Bool columns are {data: {bits, len}}; any other shape breaks decoding.
		let array = BooleanArray::from(vec![true, false, true]);
		assert_eq!(to_string(&Wrap(array)).unwrap(), r#"{"data":{"bits":[5],"len":3}}"#);
		let Wrap(back) = from_str::<Wrap>(r#"{"data":{"bits":[5],"len":3}}"#).unwrap();
		assert_eq!(values(&back), vec![true, false, true]);
	}

	#[test]
	fn serde_of_a_sliced_array_equals_a_fresh_one() {
		// A slice at a bit offset must serialize like a fresh array of the same rows.
		let all = vec![true, false, true, true, false, false, true, false, true, true];
		let view = slice(&BooleanArray::from(all.clone()), 3, 9);
		let fresh = BooleanArray::from(all[3..9].to_vec());
		assert_eq!(to_string(&Wrap(view)).unwrap(), to_string(&Wrap(fresh)).unwrap());
	}

	#[test]
	fn deserialize_rejects_short_bits() {
		// Short bits must be a serde error, not a panic on the first read.
		assert!(from_str::<Wrap>(r#"{"data":{"bits":[],"len":5}}"#).is_err());
	}
}
