// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use reifydb_codec::tag::ValueKind;
use reifydb_core::value::{batch::batch, column::builder::ColumnBuilder};
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{precision::Precision, scale::Scale},
	container::temporal_array::datetime_array,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	row_number::RowNumber,
	system_columns::{SystemColumn, with_system_column},
	time::Time,
	value_type::ValueType,
};

use crate::{error::SdkError, flow::operator::column::sink::RowSink};

pub struct InProcessRowSink {
	names: Vec<&'static str>,
	types: Vec<ValueType>,
	cols: Vec<ColumnBuilder>,
}

impl InProcessRowSink {
	pub fn new(columns: &'static [(&'static str, ValueKind)]) -> Result<Self, SdkError> {
		let mut names = Vec::with_capacity(columns.len());
		let mut types = Vec::with_capacity(columns.len());
		let mut cols = Vec::with_capacity(columns.len());
		for (name, code) in columns {
			let ty = code_to_type(*code)?;
			names.push(*name);
			cols.push(ColumnBuilder::with_capacity(ty.clone(), 0));
			types.push(ty);
		}
		Ok(Self {
			names,
			types,
			cols,
		})
	}

	pub fn finish(self, row_numbers: Vec<RowNumber>, now: DateTime) -> Result<RecordBatch, SdkError> {
		let out = self.names.into_iter().zip(self.cols).map(|(name, data)| data.finish(name)).collect();
		let mut out = batch(out)?;
		let row_count = out.num_rows();
		if !row_numbers.is_empty() {
			let row_numbers = UInt64Array::from(row_numbers.into_iter().map(|rn| rn.0).collect::<Vec<_>>());
			out = with_system_column(out, SystemColumn::RowNumbers, Arc::new(row_numbers))?;
		}
		for column in [SystemColumn::CreatedAt, SystemColumn::UpdatedAt, SystemColumn::Time] {
			out = with_system_column(out, column, Arc::new(datetime_array(vec![now; row_count])))?;
		}
		Ok(out)
	}

	#[inline]
	fn push(&mut self, col: usize, value: Value) {
		self.cols[col].push_value(value);
	}

	fn family_params_at(&self, col: usize) -> Result<(Precision, Scale), SdkError> {
		let ty = &self.types[col];
		match (ty.precision(), ty.scale()) {
			(Some(precision), Some(scale)) => Ok((precision, scale)),
			_ => Err(SdkError::InvalidInput(format!(
				"native sink column {col} of type {ty:?} is not a decimal column"
			))),
		}
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
		ValueKind::Decimal => ValueType::DECIMAL,
		other => {
			return Err(SdkError::NotImplemented(format!(
				"native sink does not support column type {:?}",
				other
			)));
		}
	})
}

impl RowSink for InProcessRowSink {
	#[inline]
	fn push_u8(&mut self, col: usize, v: u8) -> Result<(), SdkError> {
		self.push(col, Value::Uint1(v));
		Ok(())
	}
	#[inline]
	fn push_u16(&mut self, col: usize, v: u16) -> Result<(), SdkError> {
		self.push(col, Value::Uint2(v));
		Ok(())
	}
	#[inline]
	fn push_u32(&mut self, col: usize, v: u32) -> Result<(), SdkError> {
		self.push(col, Value::Uint4(v));
		Ok(())
	}
	#[inline]
	fn push_u64(&mut self, col: usize, v: u64) -> Result<(), SdkError> {
		self.push(col, Value::Uint8(v));
		Ok(())
	}
	#[inline]
	fn push_u128(&mut self, col: usize, v: u128) -> Result<(), SdkError> {
		self.push(col, Value::Uint16(v));
		Ok(())
	}
	#[inline]
	fn push_i8(&mut self, col: usize, v: i8) -> Result<(), SdkError> {
		self.push(col, Value::Int1(v));
		Ok(())
	}
	#[inline]
	fn push_i16(&mut self, col: usize, v: i16) -> Result<(), SdkError> {
		self.push(col, Value::Int2(v));
		Ok(())
	}
	#[inline]
	fn push_i32(&mut self, col: usize, v: i32) -> Result<(), SdkError> {
		self.push(col, Value::Int4(v));
		Ok(())
	}
	#[inline]
	fn push_i64(&mut self, col: usize, v: i64) -> Result<(), SdkError> {
		self.push(col, Value::Int8(v));
		Ok(())
	}
	#[inline]
	fn push_i128(&mut self, col: usize, v: i128) -> Result<(), SdkError> {
		self.push(col, Value::Int16(v));
		Ok(())
	}
	#[inline]
	fn push_f32(&mut self, col: usize, v: f32) -> Result<(), SdkError> {
		let value = OrderedF32::try_from(v).map(Value::Float4).unwrap_or(Value::None {
			inner: ValueType::Float4,
		});
		self.push(col, value);
		Ok(())
	}
	#[inline]
	fn push_f64(&mut self, col: usize, v: f64) -> Result<(), SdkError> {
		let value = OrderedF64::try_from(v).map(Value::Float8).unwrap_or(Value::None {
			inner: ValueType::Float8,
		});
		self.push(col, value);
		Ok(())
	}
	#[inline]
	fn push_date(&mut self, col: usize, v: Date) -> Result<(), SdkError> {
		self.push(col, Value::Date(v));
		Ok(())
	}
	#[inline]
	fn push_datetime(&mut self, col: usize, v: DateTime) -> Result<(), SdkError> {
		self.push(col, Value::DateTime(v));
		Ok(())
	}
	#[inline]
	fn push_time(&mut self, col: usize, v: Time) -> Result<(), SdkError> {
		self.push(col, Value::Time(v));
		Ok(())
	}
	#[inline]
	fn push_duration(&mut self, col: usize, v: Duration) -> Result<(), SdkError> {
		self.push(col, Value::Duration(v));
		Ok(())
	}
	#[inline]
	fn push_bool(&mut self, col: usize, v: bool) -> Result<(), SdkError> {
		self.push(col, Value::Boolean(v));
		Ok(())
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
	fn push_decimal(&mut self, col: usize, v: &Decimal) -> Result<(), SdkError> {
		let (precision, scale) = self.family_params_at(col)?;
		let fitted = v.fits(precision.value(), scale.value()).ok_or_else(|| {
			SdkError::InvalidInput(format!(
				"{v} does not fit native sink column {col} of precision {} and scale {}",
				precision.value(),
				scale.value()
			))
		})?;
		self.push(col, Value::Decimal(fitted));
		Ok(())
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
