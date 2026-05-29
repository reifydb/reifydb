// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_value::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

use crate::{
	error::SdkError,
	operator::{
		builder::{ColumnsBuilder, CommittedColumn},
		column::{
			row::Row,
			sink::RowSink,
			writer::{
				BlobWriter, BoolWriter, DateTimeWriter, DateWriter, DecimalWriter, DurationWriter,
				F32Writer, F64Writer, I8Writer, I16Writer, I32Writer, I64Writer, I128Writer,
				TimeWriter, U8Writer, U16Writer, U32Writer, U64Writer, U128Writer, Utf8Writer,
			},
		},
	},
};

pub struct FFIRowSink<'a> {
	writers: Vec<AnyWriter<'a>>,
	row_capacity: usize,
}

enum AnyWriter<'a> {
	U8(U8Writer<'a>),
	U16(U16Writer<'a>),
	U32(U32Writer<'a>),
	U64(U64Writer<'a>),
	U128(U128Writer<'a>),
	I8(I8Writer<'a>),
	I16(I16Writer<'a>),
	I32(I32Writer<'a>),
	I64(I64Writer<'a>),
	I128(I128Writer<'a>),
	F32(F32Writer<'a>),
	F64(F64Writer<'a>),
	Date(DateWriter<'a>),
	DateTime(DateTimeWriter<'a>),
	Time(TimeWriter<'a>),
	Duration(DurationWriter<'a>),
	Bool(BoolWriter<'a>),
	Utf8(Utf8Writer<'a>),
	Blob(BlobWriter<'a>),
	Decimal(DecimalWriter<'a>),
}

impl<'a> AnyWriter<'a> {
	fn new(
		builder: &mut ColumnsBuilder<'a>,
		type_code: ColumnTypeCode,
		row_capacity: usize,
		var_bytes_hint: usize,
	) -> Result<AnyWriter<'a>, SdkError> {
		let builder: &mut ColumnsBuilder<'a> =
			unsafe { core::mem::transmute::<&mut ColumnsBuilder<'_>, &mut ColumnsBuilder<'a>>(builder) };
		Ok(match type_code {
			ColumnTypeCode::Uint1 => AnyWriter::U8(builder.u8_writer(row_capacity)?),
			ColumnTypeCode::Uint2 => AnyWriter::U16(builder.u16_writer(row_capacity)?),
			ColumnTypeCode::Uint4 => AnyWriter::U32(builder.u32_writer(row_capacity)?),
			ColumnTypeCode::Uint8 => AnyWriter::U64(builder.u64_writer(row_capacity)?),
			ColumnTypeCode::Uint16 => AnyWriter::U128(builder.u128_writer(row_capacity)?),
			ColumnTypeCode::Int1 => AnyWriter::I8(builder.i8_writer(row_capacity)?),
			ColumnTypeCode::Int2 => AnyWriter::I16(builder.i16_writer(row_capacity)?),
			ColumnTypeCode::Int4 => AnyWriter::I32(builder.i32_writer(row_capacity)?),
			ColumnTypeCode::Int8 => AnyWriter::I64(builder.i64_writer(row_capacity)?),
			ColumnTypeCode::Int16 => AnyWriter::I128(builder.i128_writer(row_capacity)?),
			ColumnTypeCode::Float4 => AnyWriter::F32(builder.f32_writer(row_capacity)?),
			ColumnTypeCode::Float8 => AnyWriter::F64(builder.f64_writer(row_capacity)?),
			ColumnTypeCode::Date => AnyWriter::Date(builder.date_writer(row_capacity)?),
			ColumnTypeCode::DateTime => AnyWriter::DateTime(builder.datetime_writer(row_capacity)?),
			ColumnTypeCode::Time => AnyWriter::Time(builder.time_writer(row_capacity)?),
			ColumnTypeCode::Duration => AnyWriter::Duration(builder.duration_writer(row_capacity)?),
			ColumnTypeCode::Bool => AnyWriter::Bool(builder.bool_writer(row_capacity)?),
			ColumnTypeCode::Utf8 => AnyWriter::Utf8(builder.utf8_writer(row_capacity, var_bytes_hint)?),
			ColumnTypeCode::Blob => AnyWriter::Blob(builder.blob_writer(row_capacity, var_bytes_hint)?),
			ColumnTypeCode::Decimal => {
				AnyWriter::Decimal(builder.decimal_writer(row_capacity, var_bytes_hint)?)
			}
			other => {
				return Err(SdkError::Other(format!(
					"FFIRowSink: unsupported column type {:?}",
					other
				)));
			}
		})
	}

	fn finish(self) -> Result<CommittedColumn, SdkError> {
		match self {
			AnyWriter::U8(w) => w.finish(),
			AnyWriter::U16(w) => w.finish(),
			AnyWriter::U32(w) => w.finish(),
			AnyWriter::U64(w) => w.finish(),
			AnyWriter::U128(w) => w.finish(),
			AnyWriter::I8(w) => w.finish(),
			AnyWriter::I16(w) => w.finish(),
			AnyWriter::I32(w) => w.finish(),
			AnyWriter::I64(w) => w.finish(),
			AnyWriter::I128(w) => w.finish(),
			AnyWriter::F32(w) => w.finish(),
			AnyWriter::F64(w) => w.finish(),
			AnyWriter::Date(w) => w.finish(),
			AnyWriter::DateTime(w) => w.finish(),
			AnyWriter::Time(w) => w.finish(),
			AnyWriter::Duration(w) => w.finish(),
			AnyWriter::Bool(w) => w.finish(),
			AnyWriter::Utf8(w) => w.finish(),
			AnyWriter::Blob(w) => w.finish(),
			AnyWriter::Decimal(w) => w.finish(),
		}
	}
}

impl<'a> FFIRowSink<'a> {
	pub(crate) fn new<R: Row>(builder: &mut ColumnsBuilder<'a>, row_capacity: usize) -> Result<Self, SdkError> {
		let mut writers = Vec::with_capacity(R::COLUMNS.len());
		let var_count = R::COLUMNS
			.iter()
			.filter(|(_, t)| matches!(t, ColumnTypeCode::Utf8 | ColumnTypeCode::Blob))
			.count();
		let var_bytes_per = if var_count > 0 {
			(R::AVG_VAR_BYTES * row_capacity).div_ceil(var_count.max(1))
		} else {
			0
		};
		for (_, type_code) in R::COLUMNS {
			writers.push(AnyWriter::new(builder, *type_code, row_capacity, var_bytes_per)?);
		}
		Ok(Self {
			writers,
			row_capacity,
		})
	}

	#[inline]
	pub fn capacity(&self) -> usize {
		self.row_capacity
	}

	pub(crate) fn finish_all(self) -> Result<Vec<CommittedColumn>, SdkError> {
		let mut out = Vec::with_capacity(self.writers.len());
		for w in self.writers {
			out.push(w.finish()?);
		}
		Ok(out)
	}
}

impl RowSink for FFIRowSink<'_> {
	#[inline]
	fn push_u8(&mut self, col: usize, v: u8) {
		match &mut self.writers[col] {
			AnyWriter::U8(w) => w.push(v),
			_ => debug_panic("push_u8 on wrong column type"),
		}
	}

	#[inline]
	fn push_u16(&mut self, col: usize, v: u16) {
		match &mut self.writers[col] {
			AnyWriter::U16(w) => w.push(v),
			_ => debug_panic("push_u16 on wrong column type"),
		}
	}

	#[inline]
	fn push_u32(&mut self, col: usize, v: u32) {
		match &mut self.writers[col] {
			AnyWriter::U32(w) => w.push(v),
			_ => debug_panic("push_u32 on wrong column type"),
		}
	}

	#[inline]
	fn push_u64(&mut self, col: usize, v: u64) {
		match &mut self.writers[col] {
			AnyWriter::U64(w) => w.push(v),
			_ => debug_panic("push_u64 on wrong column type"),
		}
	}

	#[inline]
	fn push_u128(&mut self, col: usize, v: u128) {
		match &mut self.writers[col] {
			AnyWriter::U128(w) => w.push(v),
			_ => debug_panic("push_u128 on wrong column type"),
		}
	}

	#[inline]
	fn push_i8(&mut self, col: usize, v: i8) {
		match &mut self.writers[col] {
			AnyWriter::I8(w) => w.push(v),
			_ => debug_panic("push_i8 on wrong column type"),
		}
	}

	#[inline]
	fn push_i16(&mut self, col: usize, v: i16) {
		match &mut self.writers[col] {
			AnyWriter::I16(w) => w.push(v),
			_ => debug_panic("push_i16 on wrong column type"),
		}
	}

	#[inline]
	fn push_i32(&mut self, col: usize, v: i32) {
		match &mut self.writers[col] {
			AnyWriter::I32(w) => w.push(v),
			_ => debug_panic("push_i32 on wrong column type"),
		}
	}

	#[inline]
	fn push_i64(&mut self, col: usize, v: i64) {
		match &mut self.writers[col] {
			AnyWriter::I64(w) => w.push(v),
			_ => debug_panic("push_i64 on wrong column type"),
		}
	}

	#[inline]
	fn push_i128(&mut self, col: usize, v: i128) {
		match &mut self.writers[col] {
			AnyWriter::I128(w) => w.push(v),
			_ => debug_panic("push_i128 on wrong column type"),
		}
	}

	#[inline]
	fn push_f32(&mut self, col: usize, v: f32) {
		match &mut self.writers[col] {
			AnyWriter::F32(w) => w.push(v),
			_ => debug_panic("push_f32 on wrong column type"),
		}
	}

	#[inline]
	fn push_f64(&mut self, col: usize, v: f64) {
		match &mut self.writers[col] {
			AnyWriter::F64(w) => w.push(v),
			_ => debug_panic("push_f64 on wrong column type"),
		}
	}

	#[inline]
	fn push_date(&mut self, col: usize, v: Date) {
		match &mut self.writers[col] {
			AnyWriter::Date(w) => w.push(v),
			_ => debug_panic("push_date on wrong column type"),
		}
	}

	#[inline]
	fn push_datetime(&mut self, col: usize, v: DateTime) {
		match &mut self.writers[col] {
			AnyWriter::DateTime(w) => w.push(v),
			_ => debug_panic("push_datetime on wrong column type"),
		}
	}

	#[inline]
	fn push_time(&mut self, col: usize, v: Time) {
		match &mut self.writers[col] {
			AnyWriter::Time(w) => w.push(v),
			_ => debug_panic("push_time on wrong column type"),
		}
	}

	#[inline]
	fn push_duration(&mut self, col: usize, v: Duration) {
		match &mut self.writers[col] {
			AnyWriter::Duration(w) => w.push(v),
			_ => debug_panic("push_duration on wrong column type"),
		}
	}

	#[inline]
	fn push_bool(&mut self, col: usize, v: bool) {
		match &mut self.writers[col] {
			AnyWriter::Bool(w) => w.push(v),
			_ => debug_panic("push_bool on wrong column type"),
		}
	}

	#[inline]
	fn push_utf8(&mut self, col: usize, v: &str) -> Result<(), SdkError> {
		match &mut self.writers[col] {
			AnyWriter::Utf8(w) => w.push_str(v),
			_ => {
				debug_panic("push_utf8 on wrong column type");
				Ok(())
			}
		}
	}

	#[inline]
	fn push_blob(&mut self, col: usize, v: &[u8]) -> Result<(), SdkError> {
		match &mut self.writers[col] {
			AnyWriter::Blob(w) => w.push_bytes(v),
			_ => {
				debug_panic("push_blob on wrong column type");
				Ok(())
			}
		}
	}

	#[inline]
	fn push_decimal_bytes(&mut self, col: usize, v: &[u8]) -> Result<(), SdkError> {
		match &mut self.writers[col] {
			AnyWriter::Decimal(w) => w.push_bytes(v),
			_ => {
				debug_panic("push_decimal_bytes on wrong column type");
				Ok(())
			}
		}
	}

	#[inline]
	fn push_none(&mut self, col: usize) -> Result<(), SdkError> {
		match &mut self.writers[col] {
			AnyWriter::U8(w) => w.push_none(),
			AnyWriter::U16(w) => w.push_none(),
			AnyWriter::U32(w) => w.push_none(),
			AnyWriter::U64(w) => w.push_none(),
			AnyWriter::U128(w) => w.push_none(),
			AnyWriter::I8(w) => w.push_none(),
			AnyWriter::I16(w) => w.push_none(),
			AnyWriter::I32(w) => w.push_none(),
			AnyWriter::I64(w) => w.push_none(),
			AnyWriter::I128(w) => w.push_none(),
			AnyWriter::F32(w) => w.push_none(),
			AnyWriter::F64(w) => w.push_none(),
			AnyWriter::Date(w) => w.push_none(),
			AnyWriter::DateTime(w) => w.push_none(),
			AnyWriter::Time(w) => w.push_none(),
			AnyWriter::Duration(w) => w.push_none(),
			AnyWriter::Bool(w) => w.push_none(),
			AnyWriter::Utf8(w) => return w.push_none(),
			AnyWriter::Blob(w) => return w.push_none(),
			AnyWriter::Decimal(w) => return w.push_none(),
		}
		Ok(())
	}
}

#[inline]
fn debug_panic(msg: &'static str) {
	debug_assert!(false, "{}", msg);
}
