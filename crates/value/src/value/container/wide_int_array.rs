// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, slice::ChunksExact};

use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_buffer::MutableBuffer;

use crate::value::{Value, container::fixed_array, to_value::ToValue, value_type::ValueType};

pub trait WideInt: Copy + Default + ToValue {
	const WIDTH: usize;
	const VALUE_TYPE: ValueType;

	fn to_ordered(self, row: &mut [u8]);

	fn from_ordered(row: &[u8]) -> Self;
}

impl WideInt for i128 {
	const WIDTH: usize = 16;
	const VALUE_TYPE: ValueType = ValueType::Int16;

	fn to_ordered(self, row: &mut [u8]) {
		row.copy_from_slice(&self.to_be_bytes());
		row[0] ^= 0x80;
	}

	fn from_ordered(row: &[u8]) -> Self {
		let mut bytes = [0u8; Self::WIDTH];
		bytes.copy_from_slice(row);
		bytes[0] ^= 0x80;
		i128::from_be_bytes(bytes)
	}
}

impl WideInt for u128 {
	const WIDTH: usize = 16;
	const VALUE_TYPE: ValueType = ValueType::Uint16;

	fn to_ordered(self, row: &mut [u8]) {
		row.copy_from_slice(&self.to_be_bytes());
	}

	fn from_ordered(row: &[u8]) -> Self {
		let mut bytes = [0u8; Self::WIDTH];
		bytes.copy_from_slice(row);
		u128::from_be_bytes(bytes)
	}
}

fn rows<T: WideInt>(array: &FixedSizeBinaryArray) -> ChunksExact<'_, u8> {
	assert_eq!(
		array.value_length() as usize,
		T::WIDTH,
		"wide int column must hold {} byte values, found {}",
		T::WIDTH,
		array.value_length()
	);
	array.value_data()[..array.len() * T::WIDTH].chunks_exact(T::WIDTH)
}

pub fn wide_array<T: WideInt>(values: impl IntoIterator<Item = T>) -> FixedSizeBinaryArray {
	let values = values.into_iter();
	let mut buffer = MutableBuffer::with_capacity(values.size_hint().0 * T::WIDTH);
	for value in values {
		push_wide(&mut buffer, value);
	}
	fixed_array::from_buffer(T::WIDTH, buffer)
}

pub fn push_wide<T: WideInt>(buffer: &mut MutableBuffer, value: T) {
	let start = buffer.len();
	buffer.extend_zeros(T::WIDTH);
	value.to_ordered(&mut buffer.as_slice_mut()[start..]);
}

pub fn wide_at<T: WideInt>(array: &FixedSizeBinaryArray, index: usize) -> Option<T> {
	rows::<T>(array).nth(index).map(T::from_ordered)
}

pub fn wides<T: WideInt>(array: &FixedSizeBinaryArray) -> Vec<T> {
	rows::<T>(array).map(T::from_ordered).collect()
}

pub fn get_value<T: WideInt>(array: &FixedSizeBinaryArray, index: usize) -> Value {
	match wide_at::<T>(array, index) {
		Some(value) => value.to_value(),
		None => Value::none_of(T::VALUE_TYPE),
	}
}

pub fn as_string<T: WideInt + Display>(array: &FixedSizeBinaryArray, index: usize) -> String {
	match wide_at::<T>(array, index) {
		Some(value) => value.to_string(),
		None => "none".to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const INT16_EXTREMES: [i128; 5] = [i128::MIN, -1, 0, 1, i128::MAX];

	const UINT16_BOUNDARIES: [u128; 6] = [0, u64::MAX as u128, 1 << 64, (1 << 127) - 1, 1 << 127, u128::MAX];

	#[test]
	fn int16_round_trips_the_i128_extremes() {
		// A narrowing or a missed sign flip would clip or mirror the values only a 128 bit column can hold.
		let array = wide_array(INT16_EXTREMES);
		assert_eq!(array.value_length() as usize, 16);
		assert_eq!(wides::<i128>(&array), INT16_EXTREMES);
		for (i, value) in INT16_EXTREMES.iter().enumerate() {
			assert_eq!(wide_at::<i128>(&array, i), Some(*value));
			assert_eq!(get_value::<i128>(&array, i), Value::Int16(*value));
		}
	}

	#[test]
	fn uint16_round_trips_every_width_boundary() {
		// Values at and above 2^127 must never pick up a sign, which a signed decode of the top bit would add.
		let array = wide_array(UINT16_BOUNDARIES);
		assert_eq!(wides::<u128>(&array), UINT16_BOUNDARIES);
		for (i, value) in UINT16_BOUNDARIES.iter().enumerate() {
			assert_eq!(wide_at::<u128>(&array, i), Some(*value));
			assert_eq!(get_value::<u128>(&array, i), Value::Uint16(*value));
		}
	}

	#[test]
	fn u128_at_reads_u128_max_and_nothing_past_the_end() {
		// Reading the top bit as a sign would turn u128::MAX into a negative number or a none.
		let array = wide_array([u128::MAX]);
		assert_eq!(wide_at::<u128>(&array, 0), Some(u128::MAX));
		assert_eq!(as_string::<u128>(&array, 0), u128::MAX.to_string());
		assert_eq!(wide_at::<u128>(&array, 1), None);
		assert_eq!(get_value::<u128>(&array, 1), Value::none_of(ValueType::Uint16));
		assert_eq!(as_string::<u128>(&array, 1), "none");
		let array = wide_array([i128::MIN]);
		assert_eq!(as_string::<i128>(&array, 0), i128::MIN.to_string());
		assert_eq!(get_value::<i128>(&array, 1), Value::none_of(ValueType::Int16));
	}

	#[test]
	fn row_bytes_sort_like_the_numbers() {
		// Arrow compares these rows byte by byte, so any other order sorts and compares wide ints wrongly.
		let signed = wide_array(INT16_EXTREMES);
		for i in 1..INT16_EXTREMES.len() {
			assert!(
				signed.value(i - 1) < signed.value(i),
				"{} must sort below {}",
				INT16_EXTREMES[i - 1],
				INT16_EXTREMES[i]
			);
		}
		let unsigned = wide_array(UINT16_BOUNDARIES);
		for i in 1..UINT16_BOUNDARIES.len() {
			assert!(
				unsigned.value(i - 1) < unsigned.value(i),
				"{} must sort below {}",
				UINT16_BOUNDARIES[i - 1],
				UINT16_BOUNDARIES[i]
			);
		}
	}
}
