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
	decimal::{Decimal, unscaled},
};

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

pub fn decimal_at(array: &DecimalArray, index: usize) -> Option<Decimal> {
	let scale = array.scale().value();
	array.unscaled_at(index)
		.map(|value| Decimal::from_parts(value, scale).expect("a decimal array holds only valid decimals"))
}

pub fn decimals(array: &DecimalArray) -> Vec<Decimal> {
	(0..array.len()).filter_map(|index| decimal_at(array, index)).collect()
}

pub fn decimal_get_value(array: &DecimalArray, index: usize) -> Value {
	decimal_at(array, index).map(Value::Decimal).unwrap_or_else(Value::none)
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
mod family {
	use std::str::FromStr;

	use serde::{Deserialize, Serialize};
	use serde_json::{from_str, to_string};

	use super::*;

	#[derive(Serialize, Deserialize)]
	struct DecimalColumn(
		#[serde(serialize_with = "serialize_decimal_array", deserialize_with = "deserialize_decimal_array")]
		DecimalArray,
	);

	fn decimal(text: &str) -> Decimal {
		Decimal::from_str(text).unwrap()
	}

	#[test]
	fn width_switches_to_decimal256_past_precision_thirty_eight() {
		// Arrow rejects 39 digits in a Decimal128, so the width must follow the precision, never the values.
		let widest = decimal_array(Precision::MAX, Scale::new(10), [decimal("1.5")]);
		assert_eq!(widest.data_type(), &DataType::Decimal256(76, 10));
		assert_eq!(data_type(Precision::new(38), Scale::new(38)), DataType::Decimal128(38, 38));
		assert_eq!(data_type(Precision::MAX, Scale::MAX), DataType::Decimal256(76, 76));
	}

	#[test]
	fn precision_and_scale_are_read_back_from_the_data_type() {
		// A reader that assumes the arrow default (38, 10) would misread every value by a power of ten.
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
		// Storing 100.0 under decimal(2, 1) would break every reader that trusts the precision.
		decimal_array(Precision::new(2), Scale::new(1), [decimal("100.0")]);
	}

	#[test]
	fn reads_past_the_end_are_none() {
		// An out of range row must read as none, never as a zero or a panic.
		let dec = decimal_array(Precision::new(5), Scale::new(1), [decimal("0.5")]);
		assert_eq!(decimal_at(&dec, 1), None);
		assert_eq!(decimal_get_value(&dec, 1), Value::none());
		assert_eq!(decimal_as_string(&dec, 1), "none");
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
		let decs = DecimalColumn(decimal_array(Precision::new(40), Scale::new(5), [decimal("-12.5")]));
		let back: DecimalColumn = from_str(&to_string(&decs).unwrap()).unwrap();
		assert_eq!(decimal_as_string(&back.0, 0), "-12.50000");
		assert_eq!(back.0.data_type(), &DataType::Decimal256(40, 5));
	}

	#[test]
	fn deserialize_refuses_payloads_that_break_the_family_rules() {
		// Each payload would build an array whose readers panic or misread its values.
		assert!(from_str::<DecimalColumn>(r#"{"Decimal128":{"precision":5,"scale":6,"data":[1]}}"#).is_err());
		assert!(from_str::<DecimalColumn>(r#"{"Decimal128":{"precision":5,"scale":5,"data":[-99999]}}"#)
			.is_ok());
	}
}
