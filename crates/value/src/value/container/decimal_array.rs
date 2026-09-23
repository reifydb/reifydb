// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use arrow_array::{Decimal128Array, Decimal256Array, PrimitiveArray};
use arrow_buffer::{ScalarBuffer, i256};
use arrow_schema::DataType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::value::{Value, container::primitive};

pub const INT16_DATA_TYPE: DataType = DataType::Decimal128(38, 0);

pub const UINT16_DATA_TYPE: DataType = DataType::Decimal256(39, 0);

pub fn with_int16_type(array: Decimal128Array) -> Decimal128Array {
	array.with_data_type(INT16_DATA_TYPE)
}

pub fn with_uint16_type(array: Decimal256Array) -> Decimal256Array {
	array.with_data_type(UINT16_DATA_TYPE)
}

pub fn uint16_to_native(value: u128) -> i256 {
	i256::from_parts(value, 0)
}

pub fn uint16_from_native(value: i256) -> u128 {
	value.to_parts().0
}

pub fn int16_array(values: impl IntoIterator<Item = i128>) -> Decimal128Array {
	let values: Vec<i128> = values.into_iter().collect();
	with_int16_type(PrimitiveArray::new(ScalarBuffer::from(values), None))
}

pub fn uint16_array(values: impl IntoIterator<Item = u128>) -> Decimal256Array {
	let natives: Vec<i256> = values.into_iter().map(uint16_to_native).collect();
	with_uint16_type(PrimitiveArray::new(ScalarBuffer::from(natives), None))
}

pub fn u128_at(array: &Decimal256Array, index: usize) -> Option<u128> {
	array.values().get(index).map(|&value| uint16_from_native(value))
}

pub fn u128s(array: &Decimal256Array) -> Vec<u128> {
	array.values().iter().map(|&value| uint16_from_native(value)).collect()
}

pub fn uint16_get_value(array: &Decimal256Array, index: usize) -> Value {
	match u128_at(array, index) {
		Some(value) => Value::Uint16(value),
		None => Value::none(),
	}
}

pub fn uint16_as_string(array: &Decimal256Array, index: usize) -> String {
	match u128_at(array, index) {
		Some(value) => value.to_string(),
		None => "none".to_string(),
	}
}

pub fn deserialize_int16s<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<Decimal128Array, D::Error> {
	Ok(with_int16_type(primitive::deserialize(deserializer)?))
}

pub fn serialize_uint16s<Ser: Serializer>(array: &Decimal256Array, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	struct Rows<'a>(&'a Decimal256Array);

	impl Serialize for Rows<'_> {
		fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
			serializer.collect_seq(self.0.values().iter().map(|&value| uint16_from_native(value)))
		}
	}

	#[derive(Serialize)]
	struct Helper<'a> {
		data: Rows<'a>,
	}
	Helper {
		data: Rows(array),
	}
	.serialize(serializer)
}

pub fn deserialize_uint16s<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<Decimal256Array, D::Error> {
	#[derive(Deserialize)]
	struct Helper {
		data: Vec<u128>,
	}
	Ok(uint16_array(Helper::deserialize(deserializer)?.data))
}

#[cfg(test)]
mod tests {
	use arrow_array::Array;
	use arrow_buffer::BooleanBuffer;
	use postcard::{from_bytes, to_allocvec};
	use serde::{Deserialize, Serialize};
	use serde_json::{from_str, to_string};

	use super::*;

	#[derive(Serialize, Deserialize)]
	struct Int16Column(
		#[serde(serialize_with = "primitive::serialize", deserialize_with = "deserialize_int16s")]
		Decimal128Array,
	);

	#[derive(Serialize, Deserialize)]
	struct Uint16Column(
		#[serde(serialize_with = "serialize_uint16s", deserialize_with = "deserialize_uint16s")]
		Decimal256Array,
	);

	const UINT16_BOUNDARIES: [u128; 6] = [0, u64::MAX as u128, 1 << 64, (1 << 127) - 1, 1 << 127, u128::MAX];

	#[test]
	fn int16_round_trips_the_i128_extremes() {
		// A narrowing native would clip the values only a 128 bit column can hold.
		let values = [i128::MIN, -1, 0, 1, i128::MAX];
		let array = int16_array(values);
		assert_eq!(&array.values()[..], &values);
		for (i, value) in values.iter().enumerate() {
			assert_eq!(primitive::get_value(&array, i), Value::Int16(*value));
		}
		assert_eq!(array.data_type(), &INT16_DATA_TYPE);
	}

	#[test]
	fn uint16_round_trips_every_width_boundary() {
		// Values at and above 2^127 must never pick up a sign through the signed 256 bit native.
		let array = uint16_array(UINT16_BOUNDARIES);
		assert_eq!(u128s(&array), UINT16_BOUNDARIES);
		for (i, value) in UINT16_BOUNDARIES.iter().enumerate() {
			assert_eq!(u128_at(&array, i), Some(*value));
			assert_eq!(uint16_get_value(&array, i), Value::Uint16(*value));
			assert_eq!(uint16_from_native(uint16_to_native(*value)), *value);
			assert_eq!(uint16_to_native(*value).to_parts().1, 0);
		}
		assert_eq!(array.data_type(), &UINT16_DATA_TYPE);
	}

	#[test]
	fn u128_at_reads_u128_max_and_nothing_past_the_end() {
		// Reading the top bit as a sign would turn u128::MAX into a negative number or a none.
		let array = uint16_array([u128::MAX]);
		assert_eq!(u128_at(&array, 0), Some(u128::MAX));
		assert_eq!(uint16_as_string(&array, 0), u128::MAX.to_string());
		assert_eq!(u128_at(&array, 1), None);
		assert_eq!(uint16_get_value(&array, 1), Value::none());
		assert_eq!(uint16_as_string(&array, 1), "none");
	}

	#[test]
	fn int16_data_type_survives_every_rebuild() {
		// Any rebuild that falls back to the arrow default scale 10 misreads every value by 10^10.
		let array = int16_array([i128::MIN, 7, i128::MAX]);
		let mask = BooleanBuffer::from(vec![true, false, true]);
		let filtered = primitive::filter(&array, &mask);
		assert_eq!(&filtered.values()[..], &[i128::MIN, i128::MAX]);
		assert_eq!(filtered.data_type(), &INT16_DATA_TYPE);
		let reordered = primitive::reorder(&array, &[2, 0, 5]);
		assert_eq!(&reordered.values()[..], &[i128::MAX, i128::MIN, 0]);
		assert_eq!(reordered.data_type(), &INT16_DATA_TYPE);
		assert_eq!(primitive::slice(&array, 1, 3).data_type(), &INT16_DATA_TYPE);
		assert_eq!(primitive::take(&array, 2).data_type(), &INT16_DATA_TYPE);
		assert_eq!(
			with_int16_type(PrimitiveArray::new(ScalarBuffer::from(vec![1i128]), None)).data_type(),
			&INT16_DATA_TYPE
		);
	}

	#[test]
	fn uint16_data_type_survives_every_rebuild() {
		// Any rebuild that falls back to the arrow default (76, 10) misreads every value by 10^10.
		let array = uint16_array(UINT16_BOUNDARIES);
		let mask = BooleanBuffer::from(vec![false, true, false, true, false, true]);
		let filtered = primitive::filter(&array, &mask);
		assert_eq!(u128s(&filtered), [u64::MAX as u128, (1 << 127) - 1, u128::MAX]);
		assert_eq!(filtered.data_type(), &UINT16_DATA_TYPE);
		let reordered = primitive::reorder(&array, &[5, 0, 9]);
		assert_eq!(u128s(&reordered), [u128::MAX, 0, 0]);
		assert_eq!(reordered.data_type(), &UINT16_DATA_TYPE);
		let sliced = primitive::slice(&array, 4, 6);
		assert_eq!(u128s(&sliced), [1 << 127, u128::MAX]);
		assert_eq!(sliced.data_type(), &UINT16_DATA_TYPE);
		assert_eq!(primitive::take(&array, 2).data_type(), &UINT16_DATA_TYPE);
	}

	#[test]
	fn int16_deserialize_keeps_values_and_data_type() {
		// The generic deserialize builds the arrow default type, which must be replaced on the way in.
		let values = [i128::MIN, 0, i128::MAX];
		let bytes = to_allocvec(&Int16Column(int16_array(values))).unwrap();
		let back: Int16Column = from_bytes(&bytes).unwrap();
		assert_eq!(&back.0.values()[..], &values);
		assert_eq!(back.0.data_type(), &INT16_DATA_TYPE);
	}

	#[test]
	fn uint16_serde_writes_u128_rows_and_keeps_the_data_type() {
		// Writing the i256 native instead of the u128 value would change the wire bytes.
		let column = Uint16Column(uint16_array(UINT16_BOUNDARIES));
		let bytes = to_allocvec(&column).unwrap();
		#[derive(Serialize)]
		struct Expected {
			data: Vec<u128>,
		}
		let expected = to_allocvec(&Expected {
			data: UINT16_BOUNDARIES.to_vec(),
		})
		.unwrap();
		assert_eq!(bytes, expected);
		let back: Uint16Column = from_bytes(&bytes).unwrap();
		assert_eq!(u128s(&back.0), UINT16_BOUNDARIES);
		assert_eq!(back.0.data_type(), &UINT16_DATA_TYPE);
		let json = to_string(&column).unwrap();
		let back: Uint16Column = from_str(&json).unwrap();
		assert_eq!(u128s(&back.0), UINT16_BOUNDARIES);
	}
}
