// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, blob::Blob, date::Date, datetime::DateTime, duration::Duration, ordered_f32::OrderedF32,
		ordered_f64::OrderedF64, row_number::RowNumber, time::Time, value_type::ValueType,
	},
};

use crate::{error::SdkError, operator::column::sink::RowSink};

pub struct NativeRowSink {
	names: Vec<&'static str>,
	types: Vec<ValueType>,
	cols: Vec<ColumnBuffer>,
}

impl NativeRowSink {
	pub fn new(columns: &'static [(&'static str, ColumnTypeCode)]) -> Result<Self, SdkError> {
		let mut names = Vec::with_capacity(columns.len());
		let mut types = Vec::with_capacity(columns.len());
		let mut cols = Vec::with_capacity(columns.len());
		for (name, code) in columns {
			let ty = code_to_type(*code)?;
			names.push(*name);
			cols.push(ColumnBuffer::with_capacity(ty.clone(), 0));
			types.push(ty);
		}
		Ok(Self {
			names,
			types,
			cols,
		})
	}

	pub fn finish(self, row_numbers: Vec<RowNumber>, now_nanos: u64) -> Result<Columns, SdkError> {
		let out: Vec<ColumnWithName> = self
			.names
			.into_iter()
			.zip(self.cols)
			.map(|(name, data)| ColumnWithName {
				name: Fragment::internal(name),
				data,
			})
			.collect();
		let row_count = out.first().map_or(0, |c| c.data.len());
		let timestamps = vec![DateTime::from_nanos(now_nanos); row_count];
		Ok(Columns::with_system_columns(out, row_numbers, timestamps.clone(), timestamps))
	}

	#[inline]
	fn push(&mut self, col: usize, value: Value) {
		self.cols[col].push_value(value);
	}
}

fn code_to_type(code: ColumnTypeCode) -> Result<ValueType, SdkError> {
	Ok(match code {
		ColumnTypeCode::Bool => ValueType::Boolean,
		ColumnTypeCode::Uint1 => ValueType::Uint1,
		ColumnTypeCode::Uint2 => ValueType::Uint2,
		ColumnTypeCode::Uint4 => ValueType::Uint4,
		ColumnTypeCode::Uint8 => ValueType::Uint8,
		ColumnTypeCode::Uint16 => ValueType::Uint16,
		ColumnTypeCode::Int1 => ValueType::Int1,
		ColumnTypeCode::Int2 => ValueType::Int2,
		ColumnTypeCode::Int4 => ValueType::Int4,
		ColumnTypeCode::Int8 => ValueType::Int8,
		ColumnTypeCode::Int16 => ValueType::Int16,
		ColumnTypeCode::Float4 => ValueType::Float4,
		ColumnTypeCode::Float8 => ValueType::Float8,
		ColumnTypeCode::Date => ValueType::Date,
		ColumnTypeCode::DateTime => ValueType::DateTime,
		ColumnTypeCode::Time => ValueType::Time,
		ColumnTypeCode::Duration => ValueType::Duration,
		ColumnTypeCode::Utf8 => ValueType::Utf8,
		ColumnTypeCode::Blob => ValueType::Blob,
		other => {
			return Err(SdkError::NotImplemented(format!(
				"native sink does not support column type {:?} (Decimal and others deferred)",
				other
			)));
		}
	})
}

impl RowSink for NativeRowSink {
	#[inline]
	fn push_u8(&mut self, col: usize, v: u8) {
		self.push(col, Value::Uint1(v));
	}
	#[inline]
	fn push_u16(&mut self, col: usize, v: u16) {
		self.push(col, Value::Uint2(v));
	}
	#[inline]
	fn push_u32(&mut self, col: usize, v: u32) {
		self.push(col, Value::Uint4(v));
	}
	#[inline]
	fn push_u64(&mut self, col: usize, v: u64) {
		self.push(col, Value::Uint8(v));
	}
	#[inline]
	fn push_u128(&mut self, col: usize, v: u128) {
		self.push(col, Value::Uint16(v));
	}
	#[inline]
	fn push_i8(&mut self, col: usize, v: i8) {
		self.push(col, Value::Int1(v));
	}
	#[inline]
	fn push_i16(&mut self, col: usize, v: i16) {
		self.push(col, Value::Int2(v));
	}
	#[inline]
	fn push_i32(&mut self, col: usize, v: i32) {
		self.push(col, Value::Int4(v));
	}
	#[inline]
	fn push_i64(&mut self, col: usize, v: i64) {
		self.push(col, Value::Int8(v));
	}
	#[inline]
	fn push_i128(&mut self, col: usize, v: i128) {
		self.push(col, Value::Int16(v));
	}
	#[inline]
	fn push_f32(&mut self, col: usize, v: f32) {
		let value = OrderedF32::try_from(v).map(Value::Float4).unwrap_or(Value::None {
			inner: ValueType::Float4,
		});
		self.push(col, value);
	}
	#[inline]
	fn push_f64(&mut self, col: usize, v: f64) {
		let value = OrderedF64::try_from(v).map(Value::Float8).unwrap_or(Value::None {
			inner: ValueType::Float8,
		});
		self.push(col, value);
	}
	#[inline]
	fn push_date(&mut self, col: usize, v: Date) {
		self.push(col, Value::Date(v));
	}
	#[inline]
	fn push_datetime(&mut self, col: usize, v: DateTime) {
		self.push(col, Value::DateTime(v));
	}
	#[inline]
	fn push_time(&mut self, col: usize, v: Time) {
		self.push(col, Value::Time(v));
	}
	#[inline]
	fn push_duration(&mut self, col: usize, v: Duration) {
		self.push(col, Value::Duration(v));
	}
	#[inline]
	fn push_bool(&mut self, col: usize, v: bool) {
		self.push(col, Value::Boolean(v));
	}
	#[inline]
	fn push_utf8(&mut self, col: usize, v: &str) -> Result<(), SdkError> {
		self.push(col, Value::Utf8(v.to_string()));
		Ok(())
	}
	#[inline]
	fn push_blob(&mut self, col: usize, v: &[u8]) -> Result<(), SdkError> {
		self.push(col, Value::Blob(Blob::new(v.to_vec())));
		Ok(())
	}
	#[inline]
	fn push_decimal_bytes(&mut self, _col: usize, _v: &[u8]) -> Result<(), SdkError> {
		Err(SdkError::NotImplemented("native sink does not yet support Decimal columns".to_string()))
	}
	#[inline]
	fn push_none(&mut self, col: usize) -> Result<(), SdkError> {
		let inner = self.types[col].clone();
		self.push(
			col,
			Value::None {
				inner,
			},
		);
		Ok(())
	}
}
