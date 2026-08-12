// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::ValueKind;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, blob::Blob, date::Date, datetime::DateTime, duration::Duration, ordered_f32::OrderedF32,
		ordered_f64::OrderedF64, row_number::RowNumber, system_columns::SystemColumns, time::Time,
		value_type::ValueType,
	},
};

use crate::{error::SdkError, flow::operator::column::sink::RowSink};

pub struct InProcessRowSink {
	names: Vec<&'static str>,
	types: Vec<ValueType>,
	cols: Vec<ColumnBuffer>,
}

impl InProcessRowSink {
	pub fn new(columns: &'static [(&'static str, ValueKind)]) -> Result<Self, SdkError> {
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

	pub fn finish(self, row_numbers: Vec<RowNumber>, now: DateTime) -> Result<Columns, SdkError> {
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
		let timestamps = vec![now; row_count];
		Ok(Columns::with_system(
			out,
			SystemColumns::new(row_numbers, Vec::new(), timestamps.clone(), timestamps.clone(), timestamps),
		))
	}

	#[inline]
	fn push(&mut self, col: usize, value: Value) {
		self.cols[col].push_value(value);
	}
}

fn code_to_type(code: ValueKind) -> Result<ValueType, SdkError> {
	Ok(match code {
		ValueKind::Boolean => ValueType::Boolean,
		ValueKind::Uint1 => ValueType::Uint1,
		ValueKind::Uint2 => ValueType::Uint2,
		ValueKind::Uint4 => ValueType::Uint4,
		ValueKind::Uint8 => ValueType::Uint8,
		ValueKind::Uint16 => ValueType::Uint16,
		ValueKind::Int1 => ValueType::Int1,
		ValueKind::Int2 => ValueType::Int2,
		ValueKind::Int4 => ValueType::Int4,
		ValueKind::Int8 => ValueType::Int8,
		ValueKind::Int16 => ValueType::Int16,
		ValueKind::Float4 => ValueType::Float4,
		ValueKind::Float8 => ValueType::Float8,
		ValueKind::Date => ValueType::Date,
		ValueKind::DateTime => ValueType::DateTime,
		ValueKind::Time => ValueType::Time,
		ValueKind::Duration => ValueType::Duration,
		ValueKind::Utf8 => ValueType::Utf8,
		ValueKind::Blob => ValueType::Blob,
		other => {
			return Err(SdkError::NotImplemented(format!(
				"native sink does not support column type {:?} (Decimal and others deferred)",
				other
			)));
		}
	})
}

impl RowSink for InProcessRowSink {
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
