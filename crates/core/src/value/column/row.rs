// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Row accessor methods for Columns.
//!
//! Provides typed getters for extracting values from columns by name and row index.

use reifydb_type::{
	Blob, Date, DateTime, Decimal, Duration, IdentityId, Int, OrderedF64, RowNumber, Time, Uint, Uuid4, Uuid7,
	Value,
};

use super::Columns;

impl Columns {
	/// Get a boolean value from a column at the given row index
	pub fn get_bool(&self, name: &str, row_idx: usize) -> Option<bool> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Boolean(v) => Some(v),
			_ => None,
		})
	}

	/// Get an f32 value from a column at the given row index
	pub fn get_f32(&self, name: &str, row_idx: usize) -> Option<f32> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Float4(v) => Some(v.into()),
			_ => None,
		})
	}

	/// Get an f64 value from a column at the given row index
	pub fn get_f64(&self, name: &str, row_idx: usize) -> Option<f64> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Float8(v) => Some(v.into()),
			Value::Float4(v) => Some(f64::from(f32::from(v))),
			_ => None,
		})
	}

	/// Get a Float8 (OrderedF64) value from a column at the given row index
	pub fn get_float8(&self, name: &str, row_idx: usize) -> Option<OrderedF64> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Float8(v) => Some(v),
			_ => None,
		})
	}

	/// Get an i8 value from a column at the given row index
	pub fn get_i8(&self, name: &str, row_idx: usize) -> Option<i8> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Int1(v) => Some(v),
			_ => None,
		})
	}

	/// Get an i16 value from a column at the given row index
	pub fn get_i16(&self, name: &str, row_idx: usize) -> Option<i16> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Int2(v) => Some(v),
			Value::Int1(v) => Some(v as i16),
			_ => None,
		})
	}

	/// Get an i32 value from a column at the given row index
	pub fn get_i32(&self, name: &str, row_idx: usize) -> Option<i32> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Int4(v) => Some(v),
			Value::Int2(v) => Some(v as i32),
			Value::Int1(v) => Some(v as i32),
			_ => None,
		})
	}

	/// Get an i64 value from a column at the given row index
	pub fn get_i64(&self, name: &str, row_idx: usize) -> Option<i64> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Int8(v) => Some(v),
			Value::Int4(v) => Some(v as i64),
			Value::Int2(v) => Some(v as i64),
			Value::Int1(v) => Some(v as i64),
			_ => None,
		})
	}

	/// Get an i128 value from a column at the given row index
	pub fn get_i128(&self, name: &str, row_idx: usize) -> Option<i128> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Int16(v) => Some(v),
			Value::Int8(v) => Some(v as i128),
			Value::Int4(v) => Some(v as i128),
			Value::Int2(v) => Some(v as i128),
			Value::Int1(v) => Some(v as i128),
			_ => None,
		})
	}

	/// Get a u8 value from a column at the given row index
	pub fn get_u8(&self, name: &str, row_idx: usize) -> Option<u8> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uint1(v) => Some(v),
			_ => None,
		})
	}

	/// Get a u16 value from a column at the given row index
	pub fn get_u16(&self, name: &str, row_idx: usize) -> Option<u16> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uint2(v) => Some(v),
			Value::Uint1(v) => Some(v as u16),
			_ => None,
		})
	}

	/// Get a u32 value from a column at the given row index
	pub fn get_u32(&self, name: &str, row_idx: usize) -> Option<u32> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uint4(v) => Some(v),
			Value::Uint2(v) => Some(v as u32),
			Value::Uint1(v) => Some(v as u32),
			_ => None,
		})
	}

	/// Get a u64 value from a column at the given row index
	pub fn get_u64(&self, name: &str, row_idx: usize) -> Option<u64> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uint8(v) => Some(v),
			Value::Uint4(v) => Some(v as u64),
			Value::Uint2(v) => Some(v as u64),
			Value::Uint1(v) => Some(v as u64),
			_ => None,
		})
	}

	/// Get a u128 value from a column at the given row index
	pub fn get_u128(&self, name: &str, row_idx: usize) -> Option<u128> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uint16(v) => Some(v),
			Value::Uint8(v) => Some(v as u128),
			Value::Uint4(v) => Some(v as u128),
			Value::Uint2(v) => Some(v as u128),
			Value::Uint1(v) => Some(v as u128),
			_ => None,
		})
	}

	/// Get a UTF-8 string value from a column at the given row index
	pub fn get_string(&self, name: &str, row_idx: usize) -> Option<String> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Utf8(s) => Some(s),
			_ => None,
		})
	}

	/// Get a Date value from a column at the given row index
	pub fn get_date(&self, name: &str, row_idx: usize) -> Option<Date> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Date(v) => Some(v),
			_ => None,
		})
	}

	/// Get a DateTime value from a column at the given row index
	pub fn get_datetime(&self, name: &str, row_idx: usize) -> Option<DateTime> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::DateTime(v) => Some(v),
			_ => None,
		})
	}

	/// Get a Time value from a column at the given row index
	pub fn get_time(&self, name: &str, row_idx: usize) -> Option<Time> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Time(v) => Some(v),
			_ => None,
		})
	}

	/// Get a Duration value from a column at the given row index
	pub fn get_duration(&self, name: &str, row_idx: usize) -> Option<Duration> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Duration(v) => Some(v),
			_ => None,
		})
	}

	/// Get an IdentityId value from a column at the given row index
	pub fn get_identity_id(&self, name: &str, row_idx: usize) -> Option<IdentityId> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::IdentityId(v) => Some(v),
			_ => None,
		})
	}

	/// Get a Uuid4 value from a column at the given row index
	pub fn get_uuid4(&self, name: &str, row_idx: usize) -> Option<Uuid4> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uuid4(v) => Some(v),
			_ => None,
		})
	}

	/// Get a Uuid7 value from a column at the given row index
	pub fn get_uuid7(&self, name: &str, row_idx: usize) -> Option<Uuid7> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uuid7(v) => Some(v),
			_ => None,
		})
	}

	/// Get a Blob value from a column at the given row index
	pub fn get_blob(&self, name: &str, row_idx: usize) -> Option<Blob> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Blob(v) => Some(v),
			_ => None,
		})
	}

	/// Get an arbitrary-precision signed integer from a column at the given row index
	pub fn get_int(&self, name: &str, row_idx: usize) -> Option<Int> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Int(v) => Some(v),
			_ => None,
		})
	}

	/// Get an arbitrary-precision unsigned integer from a column at the given row index
	pub fn get_uint(&self, name: &str, row_idx: usize) -> Option<Uint> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Uint(v) => Some(v),
			_ => None,
		})
	}

	/// Get an arbitrary-precision decimal from a column at the given row index
	pub fn get_decimal(&self, name: &str, row_idx: usize) -> Option<Decimal> {
		self.column(name).and_then(|col| match col.data().get_value(row_idx) {
			Value::Decimal(v) => Some(v),
			_ => None,
		})
	}

	/// Get the raw Value from a column at the given row index
	pub fn get_value(&self, name: &str, row_idx: usize) -> Option<Value> {
		self.column(name).map(|col| col.data().get_value(row_idx))
	}

	/// Check if the value at the given column and row is undefined/null
	pub fn is_undefined(&self, name: &str, row_idx: usize) -> bool {
		self.column(name).map(|col| matches!(col.data().get_value(row_idx), Value::Undefined)).unwrap_or(true)
	}

	/// Get the row number at the given index
	pub fn get_row_number(&self, row_idx: usize) -> Option<RowNumber> {
		self.row_numbers.get(row_idx).copied()
	}
}
