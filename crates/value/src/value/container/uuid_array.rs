// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::ManuallyDrop, ops::Deref, slice};

use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_buffer::Buffer;
use uuid::Uuid;

use crate::value::{
	Value,
	identity::IdentityId,
	is::IsUuid,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
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
