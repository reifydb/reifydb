// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Borrow, result::Result as StdResult};

use arrow_array::{Array, Decimal128Array, Decimal256Array, PrimitiveArray};
use arrow_buffer::{ScalarBuffer, i256};
use arrow_schema::DataType;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::value::{
	Value,
	constraint::{precision::Precision, scale::Scale},
	container::primitive,
	decimal::{Decimal, unscaled},
	int::Int,
	uint::Uint,
};

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

pub const DECIMAL128_MAX_PRECISION: u8 = 38;

#[derive(Clone, Debug)]
pub enum DecimalArray {
	Decimal128(Decimal128Array),
	Decimal256(Decimal256Array),
}

impl DecimalArray {
	pub fn from_unscaled(precision: Precision, scale: Scale, values: impl IntoIterator<Item = i256>) -> Self {
		let values = values.into_iter().inspect(|&value| {
			assert!(
				unscaled::digits(value) <= precision.value(),
				"unscaled value {value} does not fit precision {precision}"
			)
		});
		if precision.value() <= DECIMAL128_MAX_PRECISION {
			let natives: Vec<i128> = values
				.map(|value| value.to_i128().expect("a value within precision 38 fits i128"))
				.collect();
			DecimalArray::Decimal128(
				PrimitiveArray::new(ScalarBuffer::from(natives), None)
					.with_data_type(data_type(precision, scale)),
			)
		} else {
			let natives: Vec<i256> = values.collect();
			DecimalArray::Decimal256(
				PrimitiveArray::new(ScalarBuffer::from(natives), None)
					.with_data_type(data_type(precision, scale)),
			)
		}
	}

	pub fn len(&self) -> usize {
		match self {
			DecimalArray::Decimal128(array) => array.len(),
			DecimalArray::Decimal256(array) => array.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn precision(&self) -> Precision {
		Precision::new(match self {
			DecimalArray::Decimal128(array) => array.precision(),
			DecimalArray::Decimal256(array) => array.precision(),
		})
	}

	pub fn scale(&self) -> Scale {
		let scale = match self {
			DecimalArray::Decimal128(array) => array.scale(),
			DecimalArray::Decimal256(array) => array.scale(),
		};
		Scale::new(u8::try_from(scale).expect("decimal arrays are built with a non-negative scale"))
	}

	pub fn data_type(&self) -> &DataType {
		match self {
			DecimalArray::Decimal128(array) => array.data_type(),
			DecimalArray::Decimal256(array) => array.data_type(),
		}
	}

	pub fn unscaled_at(&self, index: usize) -> Option<i256> {
		match self {
			DecimalArray::Decimal128(array) => {
				array.values().get(index).map(|&value| i256::from_i128(value))
			}
			DecimalArray::Decimal256(array) => array.values().get(index).copied(),
		}
	}

	pub fn unscaled_values(&self) -> Vec<i256> {
		match self {
			DecimalArray::Decimal128(array) => {
				array.values().iter().map(|&value| i256::from_i128(value)).collect()
			}
			DecimalArray::Decimal256(array) => array.values().to_vec(),
		}
	}
}

impl PartialEq for DecimalArray {
	fn eq(&self, other: &Self) -> bool {
		self.len() == other.len()
			&& (0..self.len()).all(|index| decimal_at(self, index) == decimal_at(other, index))
	}
}

pub fn data_type(precision: Precision, scale: Scale) -> DataType {
	if precision.value() <= DECIMAL128_MAX_PRECISION {
		DataType::Decimal128(precision.value(), scale.value() as i8)
	} else {
		DataType::Decimal256(precision.value(), scale.value() as i8)
	}
}

pub fn int_array<B: Borrow<Int>>(precision: Precision, values: impl IntoIterator<Item = B>) -> DecimalArray {
	DecimalArray::from_unscaled(precision, Scale::MIN, values.into_iter().map(|value| value.borrow().to_i256()))
}

pub fn uint_array<B: Borrow<Uint>>(precision: Precision, values: impl IntoIterator<Item = B>) -> DecimalArray {
	DecimalArray::from_unscaled(precision, Scale::MIN, values.into_iter().map(|value| value.borrow().to_i256()))
}

pub fn decimal_array<B: Borrow<Decimal>>(
	precision: Precision,
	scale: Scale,
	values: impl IntoIterator<Item = B>,
) -> DecimalArray {
	let unscaled = values.into_iter().map(|value| {
		let value = value.borrow();
		value.rescale(scale.value())
			.unwrap_or_else(|| panic!("decimal {value} does not rescale exactly to scale {scale}"))
			.unscaled()
	});
	DecimalArray::from_unscaled(precision, scale, unscaled)
}

pub fn int_at(array: &DecimalArray, index: usize) -> Option<Int> {
	array.unscaled_at(index).map(|value| Int::from_i256(value).expect("an int array holds only valid ints"))
}

pub fn uint_at(array: &DecimalArray, index: usize) -> Option<Uint> {
	array.unscaled_at(index).map(|value| Uint::from_i256(value).expect("a uint array holds only valid uints"))
}

pub fn decimal_at(array: &DecimalArray, index: usize) -> Option<Decimal> {
	let scale = array.scale().value();
	array.unscaled_at(index)
		.map(|value| Decimal::from_parts(value, scale).expect("a decimal array holds only valid decimals"))
}

pub fn ints(array: &DecimalArray) -> Vec<Int> {
	(0..array.len()).filter_map(|index| int_at(array, index)).collect()
}

pub fn uints(array: &DecimalArray) -> Vec<Uint> {
	(0..array.len()).filter_map(|index| uint_at(array, index)).collect()
}

pub fn decimals(array: &DecimalArray) -> Vec<Decimal> {
	(0..array.len()).filter_map(|index| decimal_at(array, index)).collect()
}

pub fn int_get_value(array: &DecimalArray, index: usize) -> Value {
	int_at(array, index).map(Value::Int).unwrap_or_else(Value::none)
}

pub fn uint_get_value(array: &DecimalArray, index: usize) -> Value {
	uint_at(array, index).map(Value::Uint).unwrap_or_else(Value::none)
}

pub fn decimal_get_value(array: &DecimalArray, index: usize) -> Value {
	decimal_at(array, index).map(Value::Decimal).unwrap_or_else(Value::none)
}

pub fn int_as_string(array: &DecimalArray, index: usize) -> String {
	int_at(array, index).map(|value| value.to_string()).unwrap_or_else(|| "none".to_string())
}

pub fn uint_as_string(array: &DecimalArray, index: usize) -> String {
	uint_at(array, index).map(|value| value.to_string()).unwrap_or_else(|| "none".to_string())
}

pub fn decimal_as_string(array: &DecimalArray, index: usize) -> String {
	decimal_at(array, index).map(|value| value.to_string()).unwrap_or_else(|| "none".to_string())
}

#[derive(Serialize, Deserialize)]
enum DecimalArrayWire {
	Decimal128 {
		precision: u8,
		scale: u8,
		data: Vec<i128>,
	},
	Decimal256 {
		precision: u8,
		scale: u8,
		data: Vec<(u128, i128)>,
	},
}

pub fn serialize_decimal_array<Ser: Serializer>(
	array: &DecimalArray,
	serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error> {
	let precision = array.precision().value();
	let scale = array.scale().value();
	match array {
		DecimalArray::Decimal128(array) => DecimalArrayWire::Decimal128 {
			precision,
			scale,
			data: array.values().to_vec(),
		},
		DecimalArray::Decimal256(array) => DecimalArrayWire::Decimal256 {
			precision,
			scale,
			data: array.values().iter().map(|value| value.to_parts()).collect(),
		},
	}
	.serialize(serializer)
}

pub fn deserialize_int_array<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<DecimalArray, D::Error> {
	let array = deserialize_decimal_array(deserializer)?;
	if array.scale() != Scale::MIN {
		return Err(D::Error::custom(format!("int array has scale {}, expected 0", array.scale())));
	}
	Ok(array)
}

pub fn deserialize_uint_array<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<DecimalArray, D::Error> {
	let array = deserialize_int_array(deserializer)?;
	if array.unscaled_values().iter().any(|value| value.is_negative()) {
		return Err(D::Error::custom("uint array holds a negative value"));
	}
	Ok(array)
}

pub fn deserialize_decimal_array<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<DecimalArray, D::Error> {
	let (precision, scale, data): (u8, u8, Vec<i256>) = match DecimalArrayWire::deserialize(deserializer)? {
		DecimalArrayWire::Decimal128 {
			precision,
			scale,
			data,
		} => {
			if precision > DECIMAL128_MAX_PRECISION {
				return Err(D::Error::custom(format!(
					"decimal128 array cannot have precision {precision}"
				)));
			}
			(precision, scale, data.into_iter().map(i256::from_i128).collect())
		}
		DecimalArrayWire::Decimal256 {
			precision,
			scale,
			data,
		} => {
			if precision <= DECIMAL128_MAX_PRECISION {
				return Err(D::Error::custom(format!(
					"decimal256 array cannot have precision {precision}"
				)));
			}
			(precision, scale, data.into_iter().map(|(low, high)| i256::from_parts(low, high)).collect())
		}
	};
	let precision = Precision::try_new(precision).map_err(D::Error::custom)?;
	if scale > precision.value() {
		return Err(D::Error::custom(format!("decimal array scale {scale} exceeds precision {precision}")));
	}
	if let Some(value) = data.iter().find(|&&value| unscaled::digits(value) > precision.value()) {
		return Err(D::Error::custom(format!("unscaled value {value} does not fit precision {precision}")));
	}
	Ok(DecimalArray::from_unscaled(precision, Scale::new(scale), data))
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

#[cfg(test)]
mod family {
	use std::str::FromStr;

	use serde::{Deserialize, Serialize};
	use serde_json::{from_str, to_string};

	use super::*;

	#[derive(Serialize, Deserialize)]
	struct IntColumn(
		#[serde(serialize_with = "serialize_decimal_array", deserialize_with = "deserialize_int_array")]
		DecimalArray,
	);

	#[derive(Serialize, Deserialize)]
	struct UintColumn(
		#[serde(serialize_with = "serialize_decimal_array", deserialize_with = "deserialize_uint_array")]
		DecimalArray,
	);

	#[derive(Serialize, Deserialize)]
	struct DecimalColumn(
		#[serde(serialize_with = "serialize_decimal_array", deserialize_with = "deserialize_decimal_array")]
		DecimalArray,
	);

	fn nines(digits: usize) -> Int {
		Int::from_str(&"9".repeat(digits)).unwrap()
	}

	fn decimal(text: &str) -> Decimal {
		Decimal::from_str(text).unwrap()
	}

	#[test]
	fn width_switches_to_decimal256_past_precision_thirty_eight() {
		// Arrow rejects 39 digits in a Decimal128, so the width must follow the precision, never the values.
		let narrow = int_array(Precision::new(38), [nines(38), nines(38).negate()]);
		assert!(matches!(narrow, DecimalArray::Decimal128(_)));
		assert_eq!(narrow.data_type(), &DataType::Decimal128(38, 0));
		assert_eq!(ints(&narrow), [nines(38), nines(38).negate()]);

		let wide = int_array(Precision::new(39), [Int::zero()]);
		assert!(matches!(wide, DecimalArray::Decimal256(_)));
		assert_eq!(wide.data_type(), &DataType::Decimal256(39, 0));

		let widest = decimal_array(Precision::MAX, Scale::new(10), [decimal("1.5")]);
		assert_eq!(widest.data_type(), &DataType::Decimal256(76, 10));
		assert_eq!(data_type(Precision::new(38), Scale::new(38)), DataType::Decimal128(38, 38));
		assert_eq!(data_type(Precision::MAX, Scale::MAX), DataType::Decimal256(76, 76));
	}

	#[test]
	fn precision_and_scale_are_read_back_from_the_data_type() {
		// A reader that assumes the arrow default (38, 10) would misread every value by a power of ten.
		for precision in [1, 38, 39, 76] {
			let array = uint_array(Precision::new(precision), [Uint::from_u64(9)]);
			assert_eq!(array.precision(), Precision::new(precision));
			assert_eq!(array.scale(), Scale::MIN);
		}
		let array = decimal_array(Precision::new(12), Scale::new(4), [decimal("-3.25")]);
		assert_eq!(array.precision(), Precision::new(12));
		assert_eq!(array.scale(), Scale::new(4));
		assert_eq!(array.unscaled_at(0), Some(i256::from_i128(-32500)));
		assert_eq!(decimal_as_string(&array, 0), "-3.2500");
	}

	#[test]
	fn decimals_are_rescaled_exactly_to_the_array_scale() {
		// A write that rounds would store a value the caller never wrote.
		let array = decimal_array(
			Precision::new(10),
			Scale::new(2),
			[decimal("1.5"), decimal("-0.25"), decimal("7")],
		);
		assert_eq!(array.unscaled_values(), [i256::from_i128(150), i256::from_i128(-25), i256::from_i128(700)]);
		assert_eq!(decimals(&array), [decimal("1.50"), decimal("-0.25"), decimal("7.00")]);
		assert_eq!(decimal_get_value(&array, 0).to_string(), "1.50");
	}

	#[test]
	#[should_panic(expected = "does not rescale exactly")]
	fn a_decimal_with_more_fraction_digits_than_the_scale_panics() {
		// Silently rounding 1.555 to 1.56 would hide a caller bug.
		decimal_array(Precision::new(10), Scale::new(2), [decimal("1.555")]);
	}

	#[test]
	#[should_panic(expected = "does not fit precision")]
	fn a_value_wider_than_the_precision_panics() {
		// Storing 100 under precision 2 would break every reader that trusts the precision.
		int_array(Precision::new(2), [Int::from_i64(100)]);
	}

	#[test]
	fn reads_past_the_end_are_none() {
		// An out of range row must read as none, never as a zero or a panic.
		let int = int_array(Precision::new(5), [Int::from_i64(-7)]);
		let uint = uint_array(Precision::new(50), [Uint::from_u64(7)]);
		let dec = decimal_array(Precision::new(5), Scale::new(1), [decimal("0.5")]);
		assert_eq!(int_at(&int, 1), None);
		assert_eq!(uint_at(&uint, 1), None);
		assert_eq!(decimal_at(&dec, 1), None);
		assert_eq!(int_get_value(&int, 1), Value::none());
		assert_eq!(uint_get_value(&uint, 1), Value::none());
		assert_eq!(decimal_get_value(&dec, 1), Value::none());
		assert_eq!(int_as_string(&int, 1), "none");
		assert_eq!(uint_as_string(&uint, 1), "none");
		assert_eq!(decimal_as_string(&dec, 1), "none");
		assert_eq!(int_as_string(&int, 0), "-7");
		assert_eq!(uint_as_string(&uint, 0), "7");
	}

	#[test]
	fn equality_compares_values_not_widths() {
		// Two arrays holding the same numbers must compare equal whatever width or scale stores them.
		let narrow = decimal_array(Precision::new(10), Scale::new(1), [decimal("1.5"), decimal("-2")]);
		let wide = decimal_array(Precision::new(60), Scale::new(3), [decimal("1.5"), decimal("-2")]);
		assert!(narrow == wide);
		let other = decimal_array(Precision::new(10), Scale::new(1), [decimal("1.5"), decimal("-2.1")]);
		assert!(narrow != other);
		let shorter = decimal_array(Precision::new(10), Scale::new(1), [decimal("1.5")]);
		assert!(narrow != shorter);
	}

	#[test]
	fn serde_restores_values_width_and_data_type() {
		// Deserializing into the arrow default type would misread every value by a power of ten.
		let ints_col = IntColumn(int_array(Precision::MAX, [Int::MAX, Int::MIN, Int::zero()]));
		let back: IntColumn = from_str(&to_string(&ints_col).unwrap()).unwrap();
		assert_eq!(ints(&back.0), [Int::MAX, Int::MIN, Int::zero()]);
		assert_eq!(back.0.data_type(), &DataType::Decimal256(76, 0));

		let uints_col = UintColumn(uint_array(Precision::new(38), [Uint::from_u128(10u128.pow(38) - 1)]));
		let back: UintColumn = from_bytes(&to_allocvec(&uints_col).unwrap()).unwrap();
		assert_eq!(uints(&back.0), [Uint::from_u128(10u128.pow(38) - 1)]);
		assert_eq!(back.0.data_type(), &DataType::Decimal128(38, 0));

		let decs = DecimalColumn(decimal_array(Precision::new(40), Scale::new(5), [decimal("-12.5")]));
		let back: DecimalColumn = from_str(&to_string(&decs).unwrap()).unwrap();
		assert_eq!(decimal_as_string(&back.0, 0), "-12.50000");
		assert_eq!(back.0.data_type(), &DataType::Decimal256(40, 5));
	}

	#[test]
	fn deserialize_refuses_payloads_that_break_the_family_rules() {
		// Each payload would build an array whose readers panic or misread its values.
		let bad_int = [
			r#"{"Decimal128":{"precision":5,"scale":2,"data":[1]}}"#,
			r#"{"Decimal128":{"precision":39,"scale":0,"data":[1]}}"#,
			r#"{"Decimal256":{"precision":38,"scale":0,"data":[[1,0]]}}"#,
			r#"{"Decimal128":{"precision":2,"scale":0,"data":[100]}}"#,
			r#"{"Decimal256":{"precision":77,"scale":0,"data":[[1,0]]}}"#,
			r#"{"Decimal128":{"precision":0,"scale":0,"data":[]}}"#,
		];
		for payload in bad_int {
			assert!(from_str::<IntColumn>(payload).is_err(), "int payload {payload} must be refused");
		}
		assert!(from_str::<UintColumn>(r#"{"Decimal128":{"precision":5,"scale":0,"data":[-1]}}"#).is_err());
		assert!(from_str::<UintColumn>(r#"{"Decimal128":{"precision":5,"scale":0,"data":[1]}}"#).is_ok());
		assert!(from_str::<DecimalColumn>(r#"{"Decimal128":{"precision":5,"scale":6,"data":[1]}}"#).is_err());
		assert!(from_str::<DecimalColumn>(r#"{"Decimal128":{"precision":5,"scale":5,"data":[-99999]}}"#)
			.is_ok());
	}
}
