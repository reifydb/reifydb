// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Borrow, result::Result as StdResult};

use arrow_array::{Array, LargeBinaryArray, builder::LargeBinaryBuilder};
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Deserializer, Serializer, ser::SerializeSeq};

use crate::{
	util::bitmap,
	value::{Value, container::varlen_array, value_type::ValueType},
};

fn encode(value: &Value) -> Vec<u8> {
	to_allocvec(value).expect("postcard serialization of a Value is total")
}

fn decode(row: &[u8]) -> Value {
	if row.is_empty() {
		panic!("empty Any row");
	}
	from_bytes(row).unwrap_or_else(|error| panic!("corrupt Any row {row:02x?}: {error}"))
}

pub fn any_array<B: Borrow<Value>>(values: impl IntoIterator<Item = B>) -> LargeBinaryArray {
	let mut builder = LargeBinaryBuilder::new();
	for value in values {
		builder.append_value(encode(value.borrow()));
	}
	builder.finish()
}

pub fn push_any(builder: &mut LargeBinaryBuilder, value: &Value) {
	builder.append_value(encode(value));
}

pub fn get(array: &LargeBinaryArray, index: usize) -> Option<Value> {
	varlen_array::get(array, index).map(decode)
}

pub fn values(array: &LargeBinaryArray) -> Vec<Value> {
	(0..array.len()).map(|index| decode(array.value(index))).collect()
}

pub fn get_value(array: &LargeBinaryArray, declared_type: Option<&ValueType>, index: usize) -> Value {
	match (get(array, index), declared_type) {
		(Some(value), Some(ValueType::List(_)) | Some(ValueType::Record(_))) => value,
		(Some(value), _) => Value::Any(Box::new(value)),
		(None, _) => Value::none(),
	}
}

pub fn as_string(array: &LargeBinaryArray, index: usize) -> String {
	match get(array, index) {
		Some(value) => format!("{}", value),
		None => "none".to_string(),
	}
}

pub fn reorder(array: &LargeBinaryArray, indices: &[usize]) -> LargeBinaryArray {
	let default_row = encode(&Value::none());
	let mut builder = LargeBinaryBuilder::with_capacity(indices.len(), varlen_array::compact_parts(array).0.len());
	for &index in indices {
		match varlen_array::get(array, index) {
			Some(row) => builder.append_value(row),
			None => builder.append_value(&default_row),
		}
	}
	varlen_array::attach_nulls(builder.finish(), bitmap::reorder_nulls(array.nulls(), indices))
}

pub fn equals(left: &LargeBinaryArray, right: &LargeBinaryArray) -> bool {
	left.len() == right.len()
		&& (0..left.len()).all(|index| decode(left.value(index)) == decode(right.value(index)))
}

pub fn serialize<Ser: Serializer>(array: &LargeBinaryArray, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	let mut seq = serializer.serialize_seq(Some(array.len()))?;
	for index in 0..array.len() {
		seq.serialize_element(&decode(array.value(index)))?;
	}
	seq.end()
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<LargeBinaryArray, D::Error> {
	let values: Vec<Value> = Vec::deserialize(deserializer)?;
	Ok(any_array(&values))
}

#[cfg(test)]
mod tests {
	use ::uuid::Uuid as StdUuid;
	use arrow_buffer::i256;
	use postcard::to_allocvec;
	use serde::{Deserialize, Serialize};
	use serde_json::{from_str, to_string};

	use super::*;
	use crate::value::{
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		digest::Digest,
		duration::Duration,
		identity::IdentityId,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uuid::{Uuid4, Uuid7},
	};

	#[derive(Serialize, Deserialize)]
	struct AnyColumn {
		#[serde(serialize_with = "serialize", deserialize_with = "deserialize")]
		data: LargeBinaryArray,
		#[serde(default)]
		declared_type: Option<ValueType>,
	}

	fn digest() -> Digest {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		for value in [1.0, 2.5, -4.0] {
			digest.add_value(&Value::float8(value)).unwrap();
		}
		digest
	}

	fn every_variant() -> Vec<Value> {
		vec![
			Value::none(),
			Value::none_of(ValueType::Utf8),
			Value::Boolean(true),
			Value::Float4(OrderedF32::try_from(1.5f32).unwrap()),
			Value::Float8(OrderedF64::try_from(-0.0f64).unwrap()),
			Value::Int1(-8),
			Value::Int2(-1_600),
			Value::Int4(-320_000),
			Value::Int8(-64_000_000_000),
			Value::Int16(i128::MIN),
			Value::Utf8("state".to_string()),
			Value::Uint1(8),
			Value::Uint2(1_600),
			Value::Uint4(320_000),
			Value::Uint8(64_000_000_000),
			Value::Uint16(u128::MAX),
			Value::Date(Date::new(2026, 7, 20).unwrap()),
			Value::DateTime(DateTime::new(2026, 7, 20, 12, 34, 56, 789).unwrap()),
			Value::Time(Time::new(23, 59, 59, 1).unwrap()),
			Value::Duration(Duration::new(1, 2, 3).unwrap()),
			Value::IdentityId(IdentityId(Uuid7(StdUuid::from_bytes([
				0x01, 0x8F, 0x2A, 0x3B, 0x4C, 0x5D, 0x70, 0x07, 0x80, 0x07, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x07,
			])))),
			Value::Uuid4(Uuid4(StdUuid::from_u128(4))),
			Value::Uuid7(Uuid7(StdUuid::from_u128(77))),
			Value::Blob(Blob::new(vec![1, 2, 3])),
			Value::Decimal(Decimal::from_parts(i256::from_i128(150), 2).unwrap()),
			Value::Any(Box::new(Value::Boolean(false))),
			Value::DictionaryId(DictionaryEntryId::U16(u128::MAX)),
			Value::Type(ValueType::Record(vec![("k".to_string(), ValueType::Int4)])),
			Value::List(vec![Value::Int4(1), Value::none_of(ValueType::Int4)]),
			Value::Record(vec![("k".to_string(), Value::Int8(9))]),
			Value::Tuple(vec![Value::Boolean(true), Value::none()]),
			Value::Digest(Box::new(digest())),
		]
	}

	#[test]
	fn every_value_variant_comes_back_from_its_row_exactly() {
		// A lossy row silently changes an Any cell; re-encoding catches -0.0 and 1.50 that == would hide.
		let cells = every_variant();
		let array = any_array(&cells);
		assert!(array.nulls().is_none());
		for (index, value) in cells.iter().enumerate() {
			let back = get(&array, index).unwrap();
			assert_eq!(&back, value);
			assert_eq!(to_allocvec(&back).unwrap(), to_allocvec(value).unwrap(), "row {index}");
		}
	}

	#[test]
	fn get_value_wraps_rows_unless_the_column_is_a_list_or_a_record() {
		// List and Record columns hand out the stored value; every other Any column wraps it once.
		let list = Value::List(vec![Value::Int4(1)]);
		let array = any_array([list.clone()]);
		assert_eq!(get_value(&array, None, 0), Value::Any(Box::new(list.clone())));
		assert_eq!(get_value(&array, Some(&ValueType::Any), 0), Value::Any(Box::new(list.clone())));
		assert_eq!(get_value(&array, Some(&ValueType::List(Box::new(ValueType::Int4))), 0), list);
		let record = Value::Record(vec![("a".to_string(), Value::Int4(1))]);
		let record_type = ValueType::Record(vec![("a".to_string(), ValueType::Int4)]);
		assert_eq!(get_value(&any_array([record.clone()]), Some(&record_type), 0), record);
		assert_eq!(get_value(&array, None, 1), Value::none());
	}

	#[test]
	fn a_wrapped_row_stays_wrapped() {
		// Adding or removing a wrap here would change what an Any column holds.
		let wrapped = Value::Any(Box::new(Value::Int4(3)));
		let array = any_array([wrapped.clone()]);
		assert_eq!(get(&array, 0), Some(wrapped.clone()));
		assert_eq!(get_value(&array, None, 0), Value::Any(Box::new(wrapped)));
	}

	#[test]
	fn reorder_fills_out_of_range_rows_with_the_untyped_none() {
		// Out of range rows must read as none, never as an empty row that panics on read.
		let array = reorder(&any_array([Value::Int4(1)]), &[3, 0]);
		assert_eq!(values(&array), vec![Value::none(), Value::Int4(1)]);
		assert_eq!(as_string(&array, 1), "1");
		assert_eq!(as_string(&array, 2), "none");
	}

	#[test]
	fn any_columns_are_equal_by_value_not_by_row_bytes() {
		// Postcard bytes of 1.5 and 1.50 differ; equality must still compare values.
		let one_and_a_half = |mantissa: i128, scale: u8| {
			Value::Decimal(Decimal::from_parts(i256::from_i128(mantissa), scale).unwrap())
		};
		assert!(equals(&any_array([one_and_a_half(15, 1)]), &any_array([one_and_a_half(150, 2)])));
		assert!(!equals(&any_array([Value::Int4(1)]), &any_array([Value::Int4(2)])));
		assert!(!equals(&any_array([Value::Int4(1)]), &any_array([Value::Int4(1), Value::Int4(1)])));
	}

	#[test]
	#[should_panic(expected = "empty Any row")]
	fn an_empty_any_row_panics_naming_the_type() {
		// An empty row is never written, so reading one must fail loudly instead of giving a none.
		get(&LargeBinaryArray::from_iter_values([b"".as_slice()]), 0);
	}

	#[test]
	fn serde_round_trips_every_variant_and_the_declared_type() {
		// Stored and shipped Any columns must read back exactly every variant and the declared type.
		let cells = every_variant();
		let declared_type = Some(ValueType::List(Box::new(ValueType::Int4)));
		let column = AnyColumn {
			data: any_array(&cells),
			declared_type: declared_type.clone(),
		};
		let json = to_string(&column).unwrap();
		let back: AnyColumn = from_str(&json).unwrap();
		assert_eq!(values(&back.data), cells);
		assert_eq!(back.declared_type, declared_type);
		let without_type: AnyColumn = from_str(r#"{"data":[]}"#).unwrap();
		assert_eq!(without_type.declared_type, None);
	}
}
