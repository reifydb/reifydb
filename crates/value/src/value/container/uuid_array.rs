// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::ManuallyDrop, ops::Deref, result::Result as StdResult, slice};

use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::{
	util::bitmap,
	value::{
		Value,
		identity::IdentityId,
		is::IsUuid,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

pub const UUID_WIDTH: usize = 16;

fn rows(array: &FixedSizeBinaryArray) -> &[u8] {
	assert_eq!(
		array.value_length() as usize,
		UUID_WIDTH,
		"uuid column must hold {UUID_WIDTH} byte values, found {}",
		array.value_length()
	);
	&array.value_data()[..array.len() * UUID_WIDTH]
}

pub fn uuid4s(array: &FixedSizeBinaryArray) -> &[Uuid4] {
	let bytes = rows(array);
	// SAFETY: Uuid4 is repr(transparent) over [u8; 16] with align 1 and no niche, and bytes holds exactly len rows.
	unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<Uuid4>(), array.len()) }
}

pub fn uuid7s(array: &FixedSizeBinaryArray) -> &[Uuid7] {
	let bytes = rows(array);
	// SAFETY: Uuid7 is repr(transparent) over [u8; 16] with align 1 and no niche, and bytes holds exactly len rows.
	unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<Uuid7>(), array.len()) }
}

pub fn identity_ids(array: &FixedSizeBinaryArray) -> &[IdentityId] {
	let bytes = rows(array);
	// SAFETY: IdentityId is repr(transparent) over the 16 byte, align 1 Uuid7, and bytes holds exactly len rows.
	unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<IdentityId>(), array.len()) }
}

fn collect_rows<T: IsUuid + Copy>(values: impl IntoIterator<Item = T>) -> FixedSizeBinaryArray {
	const { assert!(size_of::<T>() == UUID_WIDTH && align_of::<T>() == 1) };
	let mut rows = ManuallyDrop::new(values.into_iter().collect::<Vec<T>>());
	let (ptr, len, capacity) = (rows.as_mut_ptr(), rows.len(), rows.capacity());
	// SAFETY: T is repr(transparent) over Uuid, 16 bytes, align 1: cap * 16 u8 has its layout, any byte is valid.
	let bytes = unsafe { Vec::from_raw_parts(ptr.cast::<u8>(), len * UUID_WIDTH, capacity * UUID_WIDTH) };
	FixedSizeBinaryArray::new(UUID_WIDTH as i32, Buffer::from_vec(bytes), None)
}

pub fn from_buffer(buffer: MutableBuffer) -> FixedSizeBinaryArray {
	FixedSizeBinaryArray::new(UUID_WIDTH as i32, buffer.into(), None)
}

pub fn uuid4_array(values: impl IntoIterator<Item = Uuid4>) -> FixedSizeBinaryArray {
	collect_rows(values)
}

pub fn uuid7_array(values: impl IntoIterator<Item = Uuid7>) -> FixedSizeBinaryArray {
	collect_rows(values)
}

pub fn identity_id_array(values: impl IntoIterator<Item = IdentityId>) -> FixedSizeBinaryArray {
	collect_rows(values)
}

pub fn get_value<T>(values: &[T], index: usize) -> Value
where
	T: IsUuid + Copy + Deref<Target = Uuid>,
{
	if index < values.len() {
		values[index].to_value()
	} else {
		Value::none()
	}
}

pub fn identity_id_get_value(values: &[IdentityId], index: usize) -> Value {
	if index < values.len() {
		Value::IdentityId(values[index])
	} else {
		Value::none_of(ValueType::IdentityId)
	}
}

pub fn as_string<T: IsUuid>(values: &[T], index: usize) -> String {
	if index < values.len() {
		values[index].to_string()
	} else {
		"none".to_string()
	}
}

pub fn slice(array: &FixedSizeBinaryArray, start: usize, end: usize) -> FixedSizeBinaryArray {
	let end = end.min(array.len());
	let start = start.min(end);
	array.slice(start, end - start)
}

pub fn take(array: &FixedSizeBinaryArray, num: usize) -> FixedSizeBinaryArray {
	slice(array, 0, num)
}

pub fn filter(array: &FixedSizeBinaryArray, mask: &BooleanBuffer) -> FixedSizeBinaryArray {
	let bytes = rows(array);
	let mut kept = MutableBuffer::with_capacity(mask.count_set_bits() * UUID_WIDTH);
	for (i, keep) in mask.iter().enumerate() {
		if keep && i < array.len() {
			kept.extend_from_slice(&bytes[i * UUID_WIDTH..(i + 1) * UUID_WIDTH]);
		}
	}
	attach_nulls(from_buffer(kept), bitmap::filter_nulls(array.nulls(), mask))
}

pub fn reorder(array: &FixedSizeBinaryArray, indices: &[usize]) -> FixedSizeBinaryArray {
	let bytes = rows(array);
	let mut reordered = MutableBuffer::with_capacity(indices.len() * UUID_WIDTH);
	for &idx in indices {
		if idx < array.len() {
			reordered.extend_from_slice(&bytes[idx * UUID_WIDTH..(idx + 1) * UUID_WIDTH]);
		} else {
			reordered.extend_zeros(UUID_WIDTH);
		}
	}
	attach_nulls(from_buffer(reordered), bitmap::reorder_nulls(array.nulls(), indices))
}

pub fn attach_nulls(array: FixedSizeBinaryArray, nulls: Option<NullBuffer>) -> FixedSizeBinaryArray {
	bitmap::assert_nulls_len(nulls.as_ref(), array.len());
	let (width, values, _) = array.into_parts();
	FixedSizeBinaryArray::new(width, values, nulls)
}

pub fn capacity(array: &FixedSizeBinaryArray) -> usize {
	let buffer = array.values();
	if buffer.strong_count() == 1 {
		buffer.capacity() / UUID_WIDTH
	} else {
		array.len()
	}
}

pub fn heap_size(array: &FixedSizeBinaryArray) -> usize {
	capacity(array) * UUID_WIDTH
}

fn serialize_values<T, Ser>(data: &[T], serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
where
	T: Serialize,
	Ser: Serializer,
{
	#[derive(Serialize)]
	struct Helper<'a, T: Serialize> {
		data: &'a [T],
	}
	Helper {
		data,
	}
	.serialize(serializer)
}

fn deserialize_values<'de, T, D>(deserializer: D) -> StdResult<Vec<T>, D::Error>
where
	T: Deserialize<'de>,
	D: Deserializer<'de>,
{
	#[derive(Deserialize)]
	struct Helper<T> {
		data: Vec<T>,
	}
	Ok(Helper::<T>::deserialize(deserializer)?.data)
}

pub fn serialize_uuid4s<Ser: Serializer>(
	array: &FixedSizeBinaryArray,
	serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error> {
	serialize_values(uuid4s(array), serializer)
}

pub fn deserialize_uuid4s<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<FixedSizeBinaryArray, D::Error> {
	Ok(uuid4_array(deserialize_values::<Uuid4, D>(deserializer)?))
}

pub fn serialize_uuid7s<Ser: Serializer>(
	array: &FixedSizeBinaryArray,
	serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error> {
	serialize_values(uuid7s(array), serializer)
}

pub fn deserialize_uuid7s<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<FixedSizeBinaryArray, D::Error> {
	Ok(uuid7_array(deserialize_values::<Uuid7, D>(deserializer)?))
}

pub fn serialize_identity_ids<Ser: Serializer>(
	array: &FixedSizeBinaryArray,
	serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error> {
	serialize_values(identity_ids(array), serializer)
}

pub fn deserialize_identity_ids<'de, D: Deserializer<'de>>(
	deserializer: D,
) -> StdResult<FixedSizeBinaryArray, D::Error> {
	Ok(identity_id_array(deserialize_values::<IdentityId, D>(deserializer)?))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn uuid7_array_takes_the_vec_allocation_without_copying() {
		// Without the reinterpret, building a uuid column copies every row, 16 bytes at a time.
		let mut values = Vec::with_capacity(8);
		for i in 1..=3u128 {
			values.push(Uuid7(Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000 | i)));
		}
		assert!(values.capacity() > values.len());
		let expected = values.clone();
		let ptr = values.as_ptr().cast::<u8>();

		let array = uuid7_array(values);

		assert_eq!(array.value_data().as_ptr(), ptr);
		assert_eq!(array.len(), expected.len());
		assert_eq!(uuid7s(&array), expected.as_slice());
		for (i, value) in expected.iter().enumerate() {
			assert_eq!(array.value(i), value.as_bytes());
		}
	}
}
