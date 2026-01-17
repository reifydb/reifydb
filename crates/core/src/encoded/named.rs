// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_type::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};

use super::{
	encoded::EncodedValues,
	layout::{EncodedValuesLayout, EncodedValuesLayoutInner},
	schema::Schema,
};

/// An encoded named layout that includes field names
#[derive(Debug, Clone)]
pub struct EncodedValuesNamedLayout {
	layout: EncodedValuesLayout,
	names: Vec<String>,
	name_to_index: HashMap<String, usize>,
}

impl EncodedValuesNamedLayout {
	pub fn new(fields: impl IntoIterator<Item = (String, Type)>) -> Self {
		let (names, types): (Vec<String>, Vec<Type>) =
			fields.into_iter().map(|(name, r#type)| (name, r#type)).unzip();

		let layout = EncodedValuesLayout::testing(&types);

		let name_to_index = names.iter().enumerate().map(|(idx, name)| (name.clone(), idx)).collect();

		Self {
			layout,
			names,
			name_to_index,
		}
	}

	// === Field metadata ===

	pub fn get_name(&self, index: usize) -> Option<&str> {
		self.names.get(index).map(|s| s.as_str())
	}

	pub fn names(&self) -> &[String] {
		&self.names
	}

	pub fn field_index(&self, name: &str) -> Option<usize> {
		self.name_to_index.get(name).copied()
	}

	pub fn layout(&self) -> &EncodedValuesLayout {
		&self.layout
	}

	pub fn fields(&self) -> &EncodedValuesLayoutInner {
		&self.layout
	}

	pub fn allocate(&self) -> EncodedValues {
		self.layout.allocate()
	}

	pub fn get_value(&self, row: &EncodedValues, name: &str) -> Option<Value> {
		self.field_index(name).map(|idx| self.layout.get_value(row, idx))
	}

	pub fn set_value(&self, row: &mut EncodedValues, name: &str, value: &Value) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_value(row, idx, value))
	}

	pub fn set_values(&self, row: &mut EncodedValues, values: &[Value]) {
		debug_assert_eq!(self.layout.fields.len(), values.len());
		self.layout.set_values(row, values)
	}

	pub fn set_undefined(&self, row: &mut EncodedValues, name: &str) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_undefined(row, idx))
	}

	pub fn get_value_by_idx(&self, row: &EncodedValues, index: usize) -> Value {
		self.layout.get_value(row, index)
	}

	pub fn set_value_by_idx(&self, row: &mut EncodedValues, index: usize, value: &Value) {
		self.layout.set_value(row, index, value)
	}

	pub fn set_undefined_by_idx(&self, row: &mut EncodedValues, index: usize) {
		self.layout.set_undefined(row, index)
	}

	pub fn get_bool(&self, row: &EncodedValues, name: &str) -> Option<bool> {
		self.field_index(name).map(|idx| self.layout.get_bool(row, idx))
	}

	pub fn set_bool(&self, row: &mut EncodedValues, name: &str, value: bool) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_bool(row, idx, value))
	}

	pub fn get_i8(&self, row: &EncodedValues, name: &str) -> Option<i8> {
		self.field_index(name).map(|idx| self.layout.get_i8(row, idx))
	}

	pub fn set_i8(&self, row: &mut EncodedValues, name: &str, value: i8) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_i8(row, idx, value))
	}

	pub fn get_i16(&self, row: &EncodedValues, name: &str) -> Option<i16> {
		self.field_index(name).map(|idx| self.layout.get_i16(row, idx))
	}

	pub fn set_i16(&self, row: &mut EncodedValues, name: &str, value: i16) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_i16(row, idx, value))
	}

	pub fn get_i32(&self, row: &EncodedValues, name: &str) -> Option<i32> {
		self.field_index(name).map(|idx| self.layout.get_i32(row, idx))
	}

	pub fn set_i32(&self, row: &mut EncodedValues, name: &str, value: i32) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_i32(row, idx, value))
	}

	pub fn get_i64(&self, row: &EncodedValues, name: &str) -> Option<i64> {
		self.field_index(name).map(|idx| self.layout.get_i64(row, idx))
	}

	pub fn set_i64(&self, row: &mut EncodedValues, name: &str, value: i64) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_i64(row, idx, value))
	}

	pub fn get_i128(&self, row: &EncodedValues, name: &str) -> Option<i128> {
		self.field_index(name).map(|idx| self.layout.get_i128(row, idx))
	}

	pub fn set_i128(&self, row: &mut EncodedValues, name: &str, value: i128) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_i128(row, idx, value))
	}

	pub fn get_u8(&self, row: &EncodedValues, name: &str) -> Option<u8> {
		self.field_index(name).map(|idx| self.layout.get_u8(row, idx))
	}

	pub fn set_u8(&self, row: &mut EncodedValues, name: &str, value: u8) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_u8(row, idx, value))
	}

	pub fn get_u16(&self, row: &EncodedValues, name: &str) -> Option<u16> {
		self.field_index(name).map(|idx| self.layout.get_u16(row, idx))
	}

	pub fn set_u16(&self, row: &mut EncodedValues, name: &str, value: u16) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_u16(row, idx, value))
	}

	pub fn get_u32(&self, row: &EncodedValues, name: &str) -> Option<u32> {
		self.field_index(name).map(|idx| self.layout.get_u32(row, idx))
	}

	pub fn set_u32(&self, row: &mut EncodedValues, name: &str, value: u32) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_u32(row, idx, value))
	}

	pub fn get_u64(&self, row: &EncodedValues, name: &str) -> Option<u64> {
		self.field_index(name).map(|idx| self.layout.get_u64(row, idx))
	}

	pub fn set_u64(&self, row: &mut EncodedValues, name: &str, value: u64) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_u64(row, idx, value))
	}

	pub fn get_u128(&self, row: &EncodedValues, name: &str) -> Option<u128> {
		self.field_index(name).map(|idx| self.layout.get_u128(row, idx))
	}

	pub fn set_u128(&self, row: &mut EncodedValues, name: &str, value: u128) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_u128(row, idx, value))
	}

	pub fn get_f32(&self, row: &EncodedValues, name: &str) -> Option<f32> {
		self.field_index(name).map(|idx| self.layout.get_f32(row, idx))
	}

	pub fn set_f32(&self, row: &mut EncodedValues, name: &str, value: f32) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_f32(row, idx, value))
	}

	pub fn get_f64(&self, row: &EncodedValues, name: &str) -> Option<f64> {
		self.field_index(name).map(|idx| self.layout.get_f64(row, idx))
	}

	pub fn set_f64(&self, row: &mut EncodedValues, name: &str, value: f64) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_f64(row, idx, value))
	}

	pub fn get_utf8<'a>(&'a self, row: &'a EncodedValues, name: &str) -> Option<&'a str> {
		self.field_index(name).map(|idx| self.layout.get_utf8(row, idx))
	}

	pub fn set_utf8(&self, row: &mut EncodedValues, name: &str, value: &str) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_utf8(row, idx, value))
	}

	pub fn get_date(&self, row: &EncodedValues, name: &str) -> Option<Date> {
		self.field_index(name).map(|idx| self.layout.get_date(row, idx))
	}

	pub fn set_date(&self, row: &mut EncodedValues, name: &str, value: Date) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_date(row, idx, value))
	}

	pub fn get_datetime(&self, row: &EncodedValues, name: &str) -> Option<DateTime> {
		self.field_index(name).map(|idx| self.layout.get_datetime(row, idx))
	}

	pub fn set_datetime(&self, row: &mut EncodedValues, name: &str, value: DateTime) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_datetime(row, idx, value))
	}

	pub fn get_time(&self, row: &EncodedValues, name: &str) -> Option<Time> {
		self.field_index(name).map(|idx| self.layout.get_time(row, idx))
	}

	pub fn set_time(&self, row: &mut EncodedValues, name: &str, value: Time) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_time(row, idx, value))
	}

	pub fn get_duration(&self, row: &EncodedValues, name: &str) -> Option<Duration> {
		self.field_index(name).map(|idx| self.layout.get_duration(row, idx))
	}

	pub fn set_duration(&self, row: &mut EncodedValues, name: &str, value: Duration) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_duration(row, idx, value))
	}

	pub fn get_uuid4(&self, row: &EncodedValues, name: &str) -> Option<Uuid4> {
		self.field_index(name).map(|idx| self.layout.get_uuid4(row, idx))
	}

	pub fn set_uuid4(&self, row: &mut EncodedValues, name: &str, value: Uuid4) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_uuid4(row, idx, value))
	}

	pub fn get_uuid7(&self, row: &EncodedValues, name: &str) -> Option<Uuid7> {
		self.field_index(name).map(|idx| self.layout.get_uuid7(row, idx))
	}

	pub fn set_uuid7(&self, row: &mut EncodedValues, name: &str, value: Uuid7) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_uuid7(row, idx, value))
	}

	pub fn get_identity_id(&self, row: &EncodedValues, name: &str) -> Option<IdentityId> {
		self.field_index(name).map(|idx| self.layout.get_identity_id(row, idx))
	}

	pub fn set_identity_id(&self, row: &mut EncodedValues, name: &str, value: IdentityId) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_identity_id(row, idx, value))
	}

	pub fn get_blob(&self, row: &EncodedValues, name: &str) -> Option<Blob> {
		self.field_index(name).map(|idx| self.layout.get_blob(row, idx))
	}

	pub fn set_blob(&self, row: &mut EncodedValues, name: &str, value: &Blob) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_blob(row, idx, value))
	}

	pub fn get_decimal(&self, row: &EncodedValues, name: &str) -> Option<Decimal> {
		self.field_index(name).map(|idx| self.layout.get_decimal(row, idx))
	}

	pub fn set_decimal(&self, row: &mut EncodedValues, name: &str, value: &Decimal) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_decimal(row, idx, value))
	}

	pub fn get_int(&self, row: &EncodedValues, name: &str) -> Option<Int> {
		self.field_index(name).map(|idx| self.layout.get_int(row, idx))
	}

	pub fn set_int(&self, row: &mut EncodedValues, name: &str, value: &Int) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_int(row, idx, value))
	}

	pub fn get_uint(&self, row: &EncodedValues, name: &str) -> Option<Uint> {
		self.field_index(name).map(|idx| self.layout.get_uint(row, idx))
	}

	pub fn set_uint(&self, row: &mut EncodedValues, name: &str, value: &Uint) -> Option<()> {
		self.field_index(name).map(|idx| self.layout.set_uint(row, idx, value))
	}

	pub fn get_bool_by_idx(&self, row: &EncodedValues, index: usize) -> bool {
		self.layout.get_bool(row, index)
	}

	pub fn set_bool_by_idx(&self, row: &mut EncodedValues, index: usize, value: bool) {
		self.layout.set_bool(row, index, value)
	}

	pub fn get_i8_by_idx(&self, row: &EncodedValues, index: usize) -> i8 {
		self.layout.get_i8(row, index)
	}

	pub fn set_i8_by_idx(&self, row: &mut EncodedValues, index: usize, value: i8) {
		self.layout.set_i8(row, index, value)
	}

	pub fn get_i16_by_idx(&self, row: &EncodedValues, index: usize) -> i16 {
		self.layout.get_i16(row, index)
	}

	pub fn set_i16_by_idx(&self, row: &mut EncodedValues, index: usize, value: i16) {
		self.layout.set_i16(row, index, value)
	}

	pub fn get_i32_by_idx(&self, row: &EncodedValues, index: usize) -> i32 {
		self.layout.get_i32(row, index)
	}

	pub fn set_i32_by_idx(&self, row: &mut EncodedValues, index: usize, value: i32) {
		self.layout.set_i32(row, index, value)
	}

	pub fn get_i64_by_idx(&self, row: &EncodedValues, index: usize) -> i64 {
		self.layout.get_i64(row, index)
	}

	pub fn set_i64_by_idx(&self, row: &mut EncodedValues, index: usize, value: i64) {
		self.layout.set_i64(row, index, value)
	}

	pub fn get_i128_by_idx(&self, row: &EncodedValues, index: usize) -> i128 {
		self.layout.get_i128(row, index)
	}

	pub fn set_i128_by_idx(&self, row: &mut EncodedValues, index: usize, value: i128) {
		self.layout.set_i128(row, index, value)
	}

	pub fn get_u8_by_idx(&self, row: &EncodedValues, index: usize) -> u8 {
		self.layout.get_u8(row, index)
	}

	pub fn set_u8_by_idx(&self, row: &mut EncodedValues, index: usize, value: u8) {
		self.layout.set_u8(row, index, value)
	}

	pub fn get_u16_by_idx(&self, row: &EncodedValues, index: usize) -> u16 {
		self.layout.get_u16(row, index)
	}

	pub fn set_u16_by_idx(&self, row: &mut EncodedValues, index: usize, value: u16) {
		self.layout.set_u16(row, index, value)
	}

	pub fn get_u32_by_idx(&self, row: &EncodedValues, index: usize) -> u32 {
		self.layout.get_u32(row, index)
	}

	pub fn set_u32_by_idx(&self, row: &mut EncodedValues, index: usize, value: u32) {
		self.layout.set_u32(row, index, value)
	}

	pub fn get_u64_by_idx(&self, row: &EncodedValues, index: usize) -> u64 {
		self.layout.get_u64(row, index)
	}

	pub fn set_u64_by_idx(&self, row: &mut EncodedValues, index: usize, value: u64) {
		self.layout.set_u64(row, index, value)
	}

	pub fn get_u128_by_idx(&self, row: &EncodedValues, index: usize) -> u128 {
		self.layout.get_u128(row, index)
	}

	pub fn set_u128_by_idx(&self, row: &mut EncodedValues, index: usize, value: u128) {
		self.layout.set_u128(row, index, value)
	}

	pub fn get_f32_by_idx(&self, row: &EncodedValues, index: usize) -> f32 {
		self.layout.get_f32(row, index)
	}

	pub fn set_f32_by_idx(&self, row: &mut EncodedValues, index: usize, value: f32) {
		self.layout.set_f32(row, index, value)
	}

	pub fn get_f64_by_idx(&self, row: &EncodedValues, index: usize) -> f64 {
		self.layout.get_f64(row, index)
	}

	pub fn set_f64_by_idx(&self, row: &mut EncodedValues, index: usize, value: f64) {
		self.layout.set_f64(row, index, value)
	}

	pub fn get_utf8_by_idx<'a>(&'a self, row: &'a EncodedValues, index: usize) -> &'a str {
		self.layout.get_utf8(row, index)
	}

	pub fn set_utf8_by_idx(&self, row: &mut EncodedValues, index: usize, value: &str) {
		self.layout.set_utf8(row, index, value)
	}

	pub fn get_date_by_idx(&self, row: &EncodedValues, index: usize) -> Date {
		self.layout.get_date(row, index)
	}

	pub fn set_date_by_idx(&self, row: &mut EncodedValues, index: usize, value: Date) {
		self.layout.set_date(row, index, value)
	}

	pub fn get_datetime_by_idx(&self, row: &EncodedValues, index: usize) -> DateTime {
		self.layout.get_datetime(row, index)
	}

	pub fn set_datetime_by_idx(&self, row: &mut EncodedValues, index: usize, value: DateTime) {
		self.layout.set_datetime(row, index, value)
	}

	pub fn get_time_by_idx(&self, row: &EncodedValues, index: usize) -> Time {
		self.layout.get_time(row, index)
	}

	pub fn set_time_by_idx(&self, row: &mut EncodedValues, index: usize, value: Time) {
		self.layout.set_time(row, index, value)
	}

	pub fn get_duration_by_idx(&self, row: &EncodedValues, index: usize) -> Duration {
		self.layout.get_duration(row, index)
	}

	pub fn set_duration_by_idx(&self, row: &mut EncodedValues, index: usize, value: Duration) {
		self.layout.set_duration(row, index, value)
	}

	pub fn get_uuid4_by_idx(&self, row: &EncodedValues, index: usize) -> Uuid4 {
		self.layout.get_uuid4(row, index)
	}

	pub fn set_uuid4_by_idx(&self, row: &mut EncodedValues, index: usize, value: Uuid4) {
		self.layout.set_uuid4(row, index, value)
	}

	pub fn get_uuid7_by_idx(&self, row: &EncodedValues, index: usize) -> Uuid7 {
		self.layout.get_uuid7(row, index)
	}

	pub fn set_uuid7_by_idx(&self, row: &mut EncodedValues, index: usize, value: Uuid7) {
		self.layout.set_uuid7(row, index, value)
	}

	pub fn get_identity_id_by_idx(&self, row: &EncodedValues, index: usize) -> IdentityId {
		self.layout.get_identity_id(row, index)
	}

	pub fn set_identity_id_by_idx(&self, row: &mut EncodedValues, index: usize, value: IdentityId) {
		self.layout.set_identity_id(row, index, value)
	}

	pub fn get_blob_by_idx(&self, row: &EncodedValues, index: usize) -> Blob {
		self.layout.get_blob(row, index)
	}

	pub fn set_blob_by_idx(&self, row: &mut EncodedValues, index: usize, value: &Blob) {
		self.layout.set_blob(row, index, value)
	}

	pub fn get_decimal_by_idx(&self, row: &EncodedValues, index: usize) -> Decimal {
		self.layout.get_decimal(row, index)
	}

	pub fn set_decimal_by_idx(&self, row: &mut EncodedValues, index: usize, value: &Decimal) {
		self.layout.set_decimal(row, index, value)
	}

	pub fn get_int_by_idx(&self, row: &EncodedValues, index: usize) -> Int {
		self.layout.get_int(row, index)
	}

	pub fn set_int_by_idx(&self, row: &mut EncodedValues, index: usize, value: &Int) {
		self.layout.set_int(row, index, value)
	}

	pub fn get_uint_by_idx(&self, row: &EncodedValues, index: usize) -> Uint {
		self.layout.get_uint(row, index)
	}

	pub fn set_uint_by_idx(&self, row: &mut EncodedValues, index: usize, value: &Uint) {
		self.layout.set_uint(row, index, value)
	}

	pub fn iter_fields<'a>(&'a self, row: &'a EncodedValues) -> impl Iterator<Item = (&'a str, Value)> + 'a {
		self.names.iter().enumerate().map(|(idx, name)| {
			let value = self.layout.get_value(row, idx);
			(name.as_str(), value)
		})
	}
}

impl From<&Schema> for EncodedValuesNamedLayout {
	fn from(schema: &Schema) -> Self {
		EncodedValuesNamedLayout::new(schema.fields().iter().map(|f| (f.name.clone(), f.constraint.get_type())))
	}
}
