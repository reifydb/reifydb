// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ptr;

use reifydb_codec::tag::ValueKind;
use reifydb_value::value::{
	date::Date, datetime::DateTime, decimal::Decimal, dictionary::DictionaryEntryId, duration::Duration, time::Time,
};

use crate::{
	common::{
		extern_c::{
			binding::builder::{ColumnsBuilder, CommittedColumn},
			wire::status::EXTERN_C_OK,
		},
		family::default_family_params,
	},
	error::SdkError,
	flow::operator::{
		column::{
			row::Row,
			sink::{
				ColumnWriter, NoneOnNan, RowSink, column_mismatch, column_type, foreign_source,
				pass_after_push, pass_kind, pass_source, passed_column, unsupported_column,
			},
			writer::{
				BlobWriter, BoolWriter, DateTimeWriter, DateWriter, DecimalWriter, DictionaryIdWriter,
				DurationWriter, F32Writer, F64Writer, I8Writer, I16Writer, I32Writer, I64Writer,
				I128Writer, TimeWriter, U8Writer, U16Writer, U32Writer, U64Writer, U128Writer,
				Utf8Writer,
			},
		},
		extern_c::wire::context::ExternCContextRaw,
		view::{ColumnsView, PassSource},
	},
};

pub struct ExternCRowSink<'a> {
	ctx: *mut ExternCContextRaw,
	writers: Vec<AnyWriter<'a>>,
	columns: &'static [(&'static str, ValueKind)],
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
	DictionaryId(DictionaryIdWriter<'a>),
	Passed(CommittedColumn),
}

impl<'a> AnyWriter<'a> {
	fn new(
		builder: &mut ColumnsBuilder<'a>,
		type_code: ValueKind,
		row_capacity: usize,
		var_bytes_hint: usize,
	) -> Result<AnyWriter<'a>, SdkError> {
		// SAFETY: this only launders the outer borrow's lifetime up to `'a`; each writer copies the
		// ctx pointer and buffer handle out of the builder and never keeps a reference into it, and
		// the ExternCContextRaw behind that pointer outlives `'a`.
		let builder: &mut ColumnsBuilder<'a> =
			unsafe { core::mem::transmute::<&mut ColumnsBuilder<'_>, &mut ColumnsBuilder<'a>>(builder) };
		column_type(type_code)?;
		if let Some((precision, scale)) = default_family_params(type_code) {
			return Ok(AnyWriter::Decimal(builder.decimal_writer(row_capacity, precision, scale)?));
		}
		Ok(match type_code {
			ValueKind::Uint1 => AnyWriter::U8(builder.u8_writer(row_capacity)?),
			ValueKind::Uint2 => AnyWriter::U16(builder.u16_writer(row_capacity)?),
			ValueKind::Uint4 => AnyWriter::U32(builder.u32_writer(row_capacity)?),
			ValueKind::Uint8 => AnyWriter::U64(builder.u64_writer(row_capacity)?),
			ValueKind::Uint16 => AnyWriter::U128(builder.u128_writer(row_capacity)?),
			ValueKind::Int1 => AnyWriter::I8(builder.i8_writer(row_capacity)?),
			ValueKind::Int2 => AnyWriter::I16(builder.i16_writer(row_capacity)?),
			ValueKind::Int4 => AnyWriter::I32(builder.i32_writer(row_capacity)?),
			ValueKind::Int8 => AnyWriter::I64(builder.i64_writer(row_capacity)?),
			ValueKind::Int16 => AnyWriter::I128(builder.i128_writer(row_capacity)?),
			ValueKind::Float4 => AnyWriter::F32(builder.f32_writer(row_capacity)?),
			ValueKind::Float8 => AnyWriter::F64(builder.f64_writer(row_capacity)?),
			ValueKind::Date => AnyWriter::Date(builder.date_writer(row_capacity)?),
			ValueKind::DateTime => AnyWriter::DateTime(builder.datetime_writer(row_capacity)?),
			ValueKind::Time => AnyWriter::Time(builder.time_writer(row_capacity)?),
			ValueKind::Duration => AnyWriter::Duration(builder.duration_writer(row_capacity)?),
			ValueKind::Boolean => AnyWriter::Bool(builder.bool_writer(row_capacity)?),
			ValueKind::Utf8 => AnyWriter::Utf8(builder.utf8_writer(row_capacity, var_bytes_hint)?),
			ValueKind::Blob => AnyWriter::Blob(builder.blob_writer(row_capacity, var_bytes_hint)?),
			ValueKind::DictionaryId => AnyWriter::DictionaryId(builder.dictionary_id_writer(row_capacity)?),
			other => return Err(unsupported_column(other)),
		})
	}

	fn len(&self) -> usize {
		match self {
			AnyWriter::U8(w) => w.len(),
			AnyWriter::U16(w) => w.len(),
			AnyWriter::U32(w) => w.len(),
			AnyWriter::U64(w) => w.len(),
			AnyWriter::U128(w) => w.len(),
			AnyWriter::I8(w) => w.len(),
			AnyWriter::I16(w) => w.len(),
			AnyWriter::I32(w) => w.len(),
			AnyWriter::I64(w) => w.len(),
			AnyWriter::I128(w) => w.len(),
			AnyWriter::F32(w) => w.len(),
			AnyWriter::F64(w) => w.len(),
			AnyWriter::Date(w) => w.len(),
			AnyWriter::DateTime(w) => w.len(),
			AnyWriter::Time(w) => w.len(),
			AnyWriter::Duration(w) => w.len(),
			AnyWriter::Bool(w) => w.len(),
			AnyWriter::Utf8(w) => w.len(),
			AnyWriter::Blob(w) => w.len(),
			AnyWriter::Decimal(w) => w.len(),
			AnyWriter::DictionaryId(w) => w.len(),
			AnyWriter::Passed(c) => c.row_count(),
		}
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
			AnyWriter::DictionaryId(w) => w.finish(),
			AnyWriter::Passed(c) => Ok(c),
		}
	}
}

impl<'a> ExternCRowSink<'a> {
	pub(crate) fn new<R: Row>(
		builder: &mut ColumnsBuilder<'a>,
		ctx: *mut ExternCContextRaw,
		row_capacity: usize,
	) -> Result<Self, SdkError> {
		let mut writers = Vec::with_capacity(R::COLUMNS.len());
		let var_count =
			R::COLUMNS.iter().filter(|(_, t)| matches!(t, ValueKind::Utf8 | ValueKind::Blob)).count();
		let var_bytes_per = if var_count > 0 {
			(R::AVG_VAR_BYTES * row_capacity).div_ceil(var_count.max(1))
		} else {
			0
		};
		for (_, type_code) in R::COLUMNS {
			writers.push(AnyWriter::new(builder, *type_code, row_capacity, var_bytes_per)?);
		}
		Ok(Self {
			ctx,
			writers,
			columns: R::COLUMNS,
			row_capacity,
		})
	}

	#[inline]
	pub fn capacity(&self) -> usize {
		self.row_capacity
	}

	pub(crate) fn names(&self) -> impl ExactSizeIterator<Item = &'static str> + Clone + use<> {
		self.columns.iter().map(|(name, _)| *name)
	}

	pub(crate) fn finish_all(self) -> Result<Vec<CommittedColumn>, SdkError> {
		let mut out = Vec::with_capacity(self.writers.len());
		for w in self.writers {
			out.push(w.finish()?);
		}
		Ok(out)
	}
}

impl ExternCRowSink<'_> {
	#[inline]
	fn open(&self, col: usize) -> Result<(), SdkError> {
		match self.writers[col] {
			AnyWriter::Passed(_) => Err(passed_column(col)),
			_ => Ok(()),
		}
	}

	fn pass_nones(&mut self, col: usize, mask: &[bool]) -> Result<(), SdkError> {
		for _ in mask.iter().filter(|&&keep| keep) {
			self.push_none(col)?;
		}
		let committed = self.writers.remove(col).finish()?;
		self.writers.insert(col, AnyWriter::Passed(committed));
		Ok(())
	}
}

impl RowSink for ExternCRowSink<'_> {
	#[inline]
	fn u8_column(&mut self, col: usize) -> Result<impl ColumnWriter<u8> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::U8(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "u8")),
		}
	}

	#[inline]
	fn u16_column(&mut self, col: usize) -> Result<impl ColumnWriter<u16> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::U16(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "u16")),
		}
	}

	#[inline]
	fn u32_column(&mut self, col: usize) -> Result<impl ColumnWriter<u32> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::U32(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "u32")),
		}
	}

	#[inline]
	fn u64_column(&mut self, col: usize) -> Result<impl ColumnWriter<u64> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::U64(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "u64")),
		}
	}

	#[inline]
	fn u128_column(&mut self, col: usize) -> Result<impl ColumnWriter<u128> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::U128(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "u128")),
		}
	}

	#[inline]
	fn i8_column(&mut self, col: usize) -> Result<impl ColumnWriter<i8> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::I8(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "i8")),
		}
	}

	#[inline]
	fn i16_column(&mut self, col: usize) -> Result<impl ColumnWriter<i16> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::I16(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "i16")),
		}
	}

	#[inline]
	fn i32_column(&mut self, col: usize) -> Result<impl ColumnWriter<i32> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::I32(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "i32")),
		}
	}

	#[inline]
	fn i64_column(&mut self, col: usize) -> Result<impl ColumnWriter<i64> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::I64(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "i64")),
		}
	}

	#[inline]
	fn i128_column(&mut self, col: usize) -> Result<impl ColumnWriter<i128> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::I128(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "i128")),
		}
	}

	#[inline]
	fn f32_column(&mut self, col: usize) -> Result<impl ColumnWriter<f32> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::F32(w) => Ok(NoneOnNan(w)),
			_ => Err(column_mismatch(col, self.columns[col].1, "f32")),
		}
	}

	#[inline]
	fn f64_column(&mut self, col: usize) -> Result<impl ColumnWriter<f64> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::F64(w) => Ok(NoneOnNan(w)),
			_ => Err(column_mismatch(col, self.columns[col].1, "f64")),
		}
	}

	#[inline]
	fn date_column(&mut self, col: usize) -> Result<impl ColumnWriter<Date> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::Date(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "date")),
		}
	}

	#[inline]
	fn datetime_column(&mut self, col: usize) -> Result<impl ColumnWriter<DateTime> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::DateTime(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "datetime")),
		}
	}

	#[inline]
	fn time_column(&mut self, col: usize) -> Result<impl ColumnWriter<Time> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::Time(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "time")),
		}
	}

	#[inline]
	fn duration_column(&mut self, col: usize) -> Result<impl ColumnWriter<Duration> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::Duration(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "duration")),
		}
	}

	#[inline]
	fn bool_column(&mut self, col: usize) -> Result<impl ColumnWriter<bool> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::Bool(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "bool")),
		}
	}

	#[inline]
	fn utf8_column(&mut self, col: usize) -> Result<impl ColumnWriter<str> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::Utf8(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "utf8")),
		}
	}

	#[inline]
	fn blob_column(&mut self, col: usize) -> Result<impl ColumnWriter<[u8]> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::Blob(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "blob")),
		}
	}

	#[inline]
	fn decimal_column(&mut self, col: usize) -> Result<impl ColumnWriter<Decimal> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::Decimal(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "decimal")),
		}
	}

	#[inline]
	fn dictionary_id_column(&mut self, col: usize) -> Result<impl ColumnWriter<DictionaryEntryId> + '_, SdkError> {
		self.open(col)?;
		match &mut self.writers[col] {
			AnyWriter::DictionaryId(w) => Ok(w),
			_ => Err(column_mismatch(col, self.columns[col].1, "dictionary id")),
		}
	}

	#[inline]
	fn push_none(&mut self, col: usize) -> Result<(), SdkError> {
		self.open(col)?;
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
			AnyWriter::Utf8(w) => w.push_none(),
			AnyWriter::Blob(w) => w.push_none(),
			AnyWriter::Decimal(w) => w.push_none(),
			AnyWriter::DictionaryId(w) => w.push_none(),
			AnyWriter::Passed(_) => Err(passed_column(col)),
		}
	}

	fn pass_through(
		&mut self,
		col: usize,
		input: &impl ColumnsView,
		name: &str,
		mask: &[bool],
	) -> Result<(), SdkError> {
		self.open(col)?;
		if self.writers[col].len() != 0 {
			return Err(pass_after_push(col));
		}
		let PassSource::ExternC(wire) = pass_source(input, name, mask)? else {
			return Err(foreign_source(col));
		};
		pass_kind(col, self.columns[col].1, wire.type_code)?;
		if wire.type_code == ValueKind::None {
			return self.pass_nones(col, mask);
		}
		let mut handle = ptr::null_mut();
		// SAFETY: the host keeps the ExternCContextRaw behind `self.ctx` valid for the whole guest call;
		// `wire` borrows the live input, `mask` is `mask.len()` bytes of 0 / 1 (a bool is one byte), and
		// `handle` is a live out slot the host writes before returning.
		let code = unsafe {
			((*self.ctx).callbacks.pass_through)(
				self.ctx,
				wire,
				mask.as_ptr().cast::<u8>(),
				mask.len(),
				&mut handle,
			)
		};
		if code != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host pass_through failed with code {code}")));
		}
		let rows = mask.iter().filter(|&&keep| keep).count();
		self.writers[col] = AnyWriter::Passed(CommittedColumn::passed(handle, rows));
		Ok(())
	}
}
