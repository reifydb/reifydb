// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Borrow;

use arrow_array::{
	Array, LargeBinaryArray,
	builder::{ArrayBuilder, LargeBinaryBuilder},
};
use postcard::{from_bytes, to_allocvec};

use crate::{
	Result,
	util::{bitmap, kernel},
	value::{Value, container::varlen_array, value_type::ValueType},
};

fn encode(value: &Value, row: usize) -> Vec<u8> {
	if let Value::None {
		..
	} = value
	{
		panic!("an Any cell can not hold a none, row {row} must be a null row");
	}
	to_allocvec(value).expect("postcard serialization of a Value is total")
}

fn decode(row: &[u8]) -> Value {
	if row.is_empty() {
		panic!("empty Any row");
	}
	match from_bytes(row) {
		Ok(Value::None {
			..
		}) => panic!("corrupt Any row {row:02x?}: an encoded none, a none is a null row"),
		Ok(value) => value,
		Err(error) => panic!("corrupt Any row {row:02x?}: {error}"),
	}
}

pub fn any_array<B: Borrow<Value>>(values: impl IntoIterator<Item = B>) -> LargeBinaryArray {
	let mut builder = LargeBinaryBuilder::new();
	for (row, value) in values.into_iter().enumerate() {
		builder.append_value(encode(value.borrow(), row));
	}
	builder.finish()
}

pub fn any_array_optional<B: Borrow<Value>>(values: impl IntoIterator<Item = Option<B>>) -> LargeBinaryArray {
	let mut builder = LargeBinaryBuilder::new();
	for (row, value) in values.into_iter().enumerate() {
		match value {
			Some(value) => builder.append_value(encode(value.borrow(), row)),
			None => builder.append_null(),
		}
	}
	builder.finish()
}

pub fn push_any(builder: &mut LargeBinaryBuilder, value: &Value) {
	let row = builder.len();
	builder.append_value(encode(value, row));
}

pub fn get(array: &LargeBinaryArray, index: usize) -> Option<Value> {
	varlen_array::get(array, index).filter(|_| array.is_valid(index)).map(decode)
}

pub fn values(array: &LargeBinaryArray) -> Vec<Value> {
	(0..array.len()).map(|index| get(array, index).unwrap_or_else(Value::none)).collect()
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

pub fn reorder(array: &LargeBinaryArray, indices: &[usize]) -> Result<LargeBinaryArray> {
	kernel::rows_in_range(indices, array.len())?;
	let mut builder = LargeBinaryBuilder::with_capacity(indices.len(), varlen_array::compact_parts(array).0.len());
	for &index in indices {
		builder.append_value(array.value(index));
	}
	Ok(varlen_array::attach_nulls(builder.finish(), bitmap::reorder_nulls(array.logical_nulls().as_ref(), indices)))
}

pub fn equals(left: &LargeBinaryArray, right: &LargeBinaryArray) -> bool {
	left.len() == right.len() && (0..left.len()).all(|index| get(left, index) == get(right, index))
}

#[cfg(test)]
mod tests {
	use ::uuid::Uuid as StdUuid;
	use arrow_buffer::i256;
	use postcard::to_allocvec;

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

	fn digest() -> Digest {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		for value in [1.0, 2.5, -4.0] {
			digest.add_value(&Value::float8(value)).unwrap();
		}
		digest
	}

	fn every_variant() -> Vec<Value> {
		vec![
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
		assert!(array.logical_nulls().is_none());
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
	fn reorder_with_an_out_of_range_row_fails() {
		// An out of range row is a bug, so it must fail naming the row and length, never read as a none.
		let error = reorder(&any_array([Value::Int4(1)]), &[3, 0]).unwrap_err();
		assert_eq!(error.diagnostic().message, "row index 3 out of range for a column of 1 rows");
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
		// An empty row under a set valid bit is corrupt, so reading one must fail loudly instead of giving a
		// none.
		get(&LargeBinaryArray::from_iter_values([b"".as_slice()]), 0);
	}
}
