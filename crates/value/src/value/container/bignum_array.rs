// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Borrow, fmt::Display, marker::PhantomData, result::Result as StdResult};

use arrow_array::{Array, LargeBinaryArray, builder::LargeBinaryBuilder};
use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	ser::{SerializeSeq, SerializeStruct},
};

use crate::{
	util::bitmap,
	value::{Value, container::varlen_array, decimal::Decimal, int::Int, uint::Uint},
};

trait Row: Default + Display + Serialize {
	fn encode(&self, out: &mut Vec<u8>);

	fn decode(row: &[u8]) -> Self;

	fn into_value(self) -> Value;
}

impl Row for Int {
	fn encode(&self, out: &mut Vec<u8>) {
		self.encode_row(out);
	}

	fn decode(row: &[u8]) -> Self {
		Int::decode_row(row)
	}

	fn into_value(self) -> Value {
		Value::Int(self)
	}
}

impl Row for Uint {
	fn encode(&self, out: &mut Vec<u8>) {
		self.encode_row(out);
	}

	fn decode(row: &[u8]) -> Self {
		Uint::decode_row(row)
	}

	fn into_value(self) -> Value {
		Value::Uint(self)
	}
}

impl Row for Decimal {
	fn encode(&self, out: &mut Vec<u8>) {
		self.encode_row(out);
	}

	fn decode(row: &[u8]) -> Self {
		Decimal::decode_row(row)
	}

	fn into_value(self) -> Value {
		Value::Decimal(self)
	}
}

fn row_array<T: Row, B: Borrow<T>>(values: impl IntoIterator<Item = B>) -> LargeBinaryArray {
	let mut builder = LargeBinaryBuilder::new();
	let mut row = Vec::new();
	for value in values {
		row.clear();
		value.borrow().encode(&mut row);
		builder.append_value(&row);
	}
	builder.finish()
}

fn push<T: Row>(builder: &mut LargeBinaryBuilder, value: &T) {
	let mut row = Vec::new();
	value.encode(&mut row);
	builder.append_value(row);
}

fn at<T: Row>(array: &LargeBinaryArray, index: usize) -> Option<T> {
	varlen_array::get(array, index).map(T::decode)
}

fn rows<T: Row>(array: &LargeBinaryArray) -> Vec<T> {
	(0..array.len()).map(|index| T::decode(array.value(index))).collect()
}

fn get_value<T: Row>(array: &LargeBinaryArray, index: usize) -> Value {
	match at::<T>(array, index) {
		Some(value) => value.into_value(),
		None => Value::none(),
	}
}

fn as_string<T: Row>(array: &LargeBinaryArray, index: usize) -> String {
	match at::<T>(array, index) {
		Some(value) => value.to_string(),
		None => "none".to_string(),
	}
}

fn reorder<T: Row>(array: &LargeBinaryArray, indices: &[usize]) -> LargeBinaryArray {
	let mut default_row = Vec::new();
	T::default().encode(&mut default_row);
	let mut builder = LargeBinaryBuilder::with_capacity(indices.len(), varlen_array::compact_parts(array).0.len());
	for &index in indices {
		match varlen_array::get(array, index) {
			Some(row) => builder.append_value(row),
			None => builder.append_value(&default_row),
		}
	}
	varlen_array::attach_nulls(builder.finish(), bitmap::reorder_nulls(array.nulls(), indices))
}

struct Rows<'a, T> {
	array: &'a LargeBinaryArray,
	row: PhantomData<T>,
}

impl<T: Row> Serialize for Rows<'_, T> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		let mut seq = serializer.serialize_seq(Some(self.array.len()))?;
		for index in 0..self.array.len() {
			seq.serialize_element(&T::decode(self.array.value(index)))?;
		}
		seq.end()
	}
}

fn serialize<T: Row, Ser: Serializer>(array: &LargeBinaryArray, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	let mut state = serializer.serialize_struct("Helper", 1)?;
	state.serialize_field(
		"data",
		&Rows::<T> {
			array,
			row: PhantomData,
		},
	)?;
	state.end()
}

fn deserialize<'de, T, D>(deserializer: D) -> StdResult<LargeBinaryArray, D::Error>
where
	T: Row + Deserialize<'de>,
	D: Deserializer<'de>,
{
	#[derive(Deserialize)]
	struct Helper<T> {
		data: Vec<T>,
	}
	Ok(row_array::<T, T>(Helper::<T>::deserialize(deserializer)?.data))
}

pub fn int_array<B: Borrow<Int>>(values: impl IntoIterator<Item = B>) -> LargeBinaryArray {
	row_array::<Int, B>(values)
}

pub fn uint_array<B: Borrow<Uint>>(values: impl IntoIterator<Item = B>) -> LargeBinaryArray {
	row_array::<Uint, B>(values)
}

pub fn decimal_array<B: Borrow<Decimal>>(values: impl IntoIterator<Item = B>) -> LargeBinaryArray {
	row_array::<Decimal, B>(values)
}

pub fn push_int(builder: &mut LargeBinaryBuilder, value: &Int) {
	push(builder, value);
}

pub fn push_uint(builder: &mut LargeBinaryBuilder, value: &Uint) {
	push(builder, value);
}

pub fn push_decimal(builder: &mut LargeBinaryBuilder, value: &Decimal) {
	push(builder, value);
}

pub fn int_at(array: &LargeBinaryArray, index: usize) -> Option<Int> {
	at(array, index)
}

pub fn uint_at(array: &LargeBinaryArray, index: usize) -> Option<Uint> {
	at(array, index)
}

pub fn decimal_at(array: &LargeBinaryArray, index: usize) -> Option<Decimal> {
	at(array, index)
}

pub fn ints(array: &LargeBinaryArray) -> Vec<Int> {
	rows(array)
}

pub fn uints(array: &LargeBinaryArray) -> Vec<Uint> {
	rows(array)
}

pub fn decimals(array: &LargeBinaryArray) -> Vec<Decimal> {
	rows(array)
}

pub fn int_get_value(array: &LargeBinaryArray, index: usize) -> Value {
	get_value::<Int>(array, index)
}

pub fn uint_get_value(array: &LargeBinaryArray, index: usize) -> Value {
	get_value::<Uint>(array, index)
}

pub fn decimal_get_value(array: &LargeBinaryArray, index: usize) -> Value {
	get_value::<Decimal>(array, index)
}

pub fn int_as_string(array: &LargeBinaryArray, index: usize) -> String {
	as_string::<Int>(array, index)
}

pub fn uint_as_string(array: &LargeBinaryArray, index: usize) -> String {
	as_string::<Uint>(array, index)
}

pub fn decimal_as_string(array: &LargeBinaryArray, index: usize) -> String {
	as_string::<Decimal>(array, index)
}

pub fn reorder_ints(array: &LargeBinaryArray, indices: &[usize]) -> LargeBinaryArray {
	reorder::<Int>(array, indices)
}

pub fn reorder_uints(array: &LargeBinaryArray, indices: &[usize]) -> LargeBinaryArray {
	reorder::<Uint>(array, indices)
}

pub fn reorder_decimals(array: &LargeBinaryArray, indices: &[usize]) -> LargeBinaryArray {
	reorder::<Decimal>(array, indices)
}

pub fn decimals_equal(left: &LargeBinaryArray, right: &LargeBinaryArray) -> bool {
	left.len() == right.len()
		&& (0..left.len()).all(|index| {
			Decimal::row_value_prefix(left.value(index)) == Decimal::row_value_prefix(right.value(index))
		})
}

pub fn serialize_ints<Ser: Serializer>(array: &LargeBinaryArray, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	serialize::<Int, Ser>(array, serializer)
}

pub fn deserialize_ints<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<LargeBinaryArray, D::Error> {
	deserialize::<Int, D>(deserializer)
}

pub fn serialize_uints<Ser: Serializer>(array: &LargeBinaryArray, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	serialize::<Uint, Ser>(array, serializer)
}

pub fn deserialize_uints<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<LargeBinaryArray, D::Error> {
	deserialize::<Uint, D>(deserializer)
}

pub fn serialize_decimals<Ser: Serializer>(
	array: &LargeBinaryArray,
	serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error> {
	serialize::<Decimal, Ser>(array, serializer)
}

pub fn deserialize_decimals<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<LargeBinaryArray, D::Error> {
	deserialize::<Decimal, D>(deserializer)
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use num_bigint::BigInt;
	use postcard::{from_bytes, to_allocvec};
	use serde_json::{from_str, to_string};

	use super::*;

	#[derive(Serialize, Deserialize)]
	struct IntColumn(
		#[serde(serialize_with = "serialize_ints", deserialize_with = "deserialize_ints")] LargeBinaryArray,
	);

	#[derive(Serialize, Deserialize)]
	struct DecimalColumn(
		#[serde(serialize_with = "serialize_decimals", deserialize_with = "deserialize_decimals")]
		LargeBinaryArray,
	);

	fn dec(text: &str) -> Decimal {
		Decimal::from_str(text).unwrap()
	}

	fn big(text: &str) -> Int {
		Int(BigInt::from_str(text).unwrap())
	}

	#[test]
	fn helper_built_arrays_carry_no_null_buffer() {
		// Arrays built from plain values must carry no validity, otherwise they turn nullable.
		assert!(int_array([Int::from(1)]).nulls().is_none());
		assert!(uint_array([Uint::from(1u8)]).nulls().is_none());
		assert!(decimal_array([dec("1.5")]).nulls().is_none());
		assert!(reorder_ints(&int_array([Int::from(1)]), &[0, 5]).nulls().is_none());
	}

	#[test]
	fn reorder_fills_out_of_range_rows_with_zero_of_scale_zero() {
		// Out of range rows must read as zero, never as an empty row that panics on read.
		let reordered = reorder_ints(&int_array([Int::from(7), Int::from(-3)]), &[1, 9, 0]);
		assert_eq!(ints(&reordered), vec![Int::from(-3), Int::zero(), Int::from(7)]);
		let reordered = reorder_uints(&uint_array([Uint::from(7u8)]), &[3]);
		assert_eq!(uints(&reordered), vec![Uint::zero()]);
		let reordered = reorder_decimals(&decimal_array([dec("1.50")]), &[2, 0]);
		let parts: Vec<_> = decimals(&reordered).iter().map(|d| d.0.as_bigint_and_exponent()).collect();
		assert_eq!(parts, vec![(BigInt::from(0), 0), (BigInt::from(150), 2)]);
	}

	#[test]
	fn reads_past_the_end_give_an_untyped_none_and_the_none_text() {
		// A read past the end must give none, never a panic.
		let array = int_array([Int::from(1)]);
		assert_eq!(int_at(&array, 1), None);
		assert_eq!(int_get_value(&array, 0), Value::Int(Int::from(1)));
		assert_eq!(int_get_value(&array, 1), Value::none());
		assert_eq!(int_as_string(&array, 1), "none");
		let decimals = decimal_array([dec("-0.120")]);
		assert_eq!(decimal_as_string(&decimals, 0), "-0.120");
		assert_eq!(decimal_get_value(&decimals, 3), Value::none());
		assert_eq!(uint_get_value(&uint_array([Uint::from(9u8)]), 0), Value::Uint(Uint::from(9u8)));
	}

	#[test]
	fn decimal_columns_are_equal_by_value_not_by_scale() {
		// Byte equality would make [1.5] != [1.50]; equality must compare values, never scales.
		assert!(decimals_equal(
			&decimal_array([dec("1.5"), dec("-2")]),
			&decimal_array([dec("1.50"), dec("-2.000")])
		));
		assert!(!decimals_equal(&decimal_array([dec("1.5")]), &decimal_array([dec("1.6")])));
		assert!(!decimals_equal(&decimal_array([dec("1.5")]), &decimal_array([dec("1.5"), dec("1.5")])));
		assert!(decimals_equal(&decimal_array([dec("0")]), &decimal_array([dec("0.00")])));
	}

	#[test]
	fn int_serde_round_trips() {
		// Stored and shipped columns must read back exactly the rows that were written.
		let values = vec![big("-340282366920938463463374607431768211456"), Int::zero(), big("255"), big("256")];
		let array = int_array(&values);
		let back: IntColumn = from_bytes(&to_allocvec(&IntColumn(array.clone())).unwrap()).unwrap();
		assert_eq!(ints(&back.0), values);
	}

	#[test]
	fn decimal_serde_round_trip_keeps_the_scale() {
		// A decode that normalised 1.50 to 1.5 would pass Decimal == but change the stored text.
		let values = vec![dec("1.50"), dec("-123.4500"), dec("0.0000001"), dec("12345678901234567890.25")];
		let array = decimal_array(&values);
		let json = to_string(&DecimalColumn(array)).unwrap();
		let back: DecimalColumn = from_str(&json).unwrap();
		let texts: Vec<String> = decimals(&back.0).iter().map(|d| d.to_string()).collect();
		assert_eq!(texts, values.iter().map(|d| d.to_string()).collect::<Vec<_>>());
		assert_eq!(texts[0], "1.50");
	}
}
