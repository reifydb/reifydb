// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod in_process;

use reifydb_codec::tag::{TypeTag, ValueKind};
use reifydb_value::value::{
	date::Date, datetime::DateTime, decimal::Decimal, dictionary::DictionaryEntryId, duration::Duration,
	time::Time, value_type::ValueType,
};

use crate::error::SdkError;

pub trait ColumnWriter<T: ?Sized> {
	fn push(&mut self, value: &T) -> Result<(), SdkError>;
	fn push_none(&mut self) -> Result<(), SdkError>;
}

impl<T: ?Sized, W: ColumnWriter<T> + ?Sized> ColumnWriter<T> for &mut W {
	#[inline]
	fn push(&mut self, value: &T) -> Result<(), SdkError> {
		(**self).push(value)
	}

	#[inline]
	fn push_none(&mut self) -> Result<(), SdkError> {
		(**self).push_none()
	}
}

pub trait RowSink {
	fn u8_column(&mut self, col: usize) -> Result<impl ColumnWriter<u8> + '_, SdkError>;
	fn u16_column(&mut self, col: usize) -> Result<impl ColumnWriter<u16> + '_, SdkError>;
	fn u32_column(&mut self, col: usize) -> Result<impl ColumnWriter<u32> + '_, SdkError>;
	fn u64_column(&mut self, col: usize) -> Result<impl ColumnWriter<u64> + '_, SdkError>;
	fn u128_column(&mut self, col: usize) -> Result<impl ColumnWriter<u128> + '_, SdkError>;
	fn i8_column(&mut self, col: usize) -> Result<impl ColumnWriter<i8> + '_, SdkError>;
	fn i16_column(&mut self, col: usize) -> Result<impl ColumnWriter<i16> + '_, SdkError>;
	fn i32_column(&mut self, col: usize) -> Result<impl ColumnWriter<i32> + '_, SdkError>;
	fn i64_column(&mut self, col: usize) -> Result<impl ColumnWriter<i64> + '_, SdkError>;
	fn i128_column(&mut self, col: usize) -> Result<impl ColumnWriter<i128> + '_, SdkError>;
	fn f32_column(&mut self, col: usize) -> Result<impl ColumnWriter<f32> + '_, SdkError>;
	fn f64_column(&mut self, col: usize) -> Result<impl ColumnWriter<f64> + '_, SdkError>;
	fn date_column(&mut self, col: usize) -> Result<impl ColumnWriter<Date> + '_, SdkError>;
	fn datetime_column(&mut self, col: usize) -> Result<impl ColumnWriter<DateTime> + '_, SdkError>;
	fn time_column(&mut self, col: usize) -> Result<impl ColumnWriter<Time> + '_, SdkError>;
	fn duration_column(&mut self, col: usize) -> Result<impl ColumnWriter<Duration> + '_, SdkError>;
	fn bool_column(&mut self, col: usize) -> Result<impl ColumnWriter<bool> + '_, SdkError>;
	fn utf8_column(&mut self, col: usize) -> Result<impl ColumnWriter<str> + '_, SdkError>;
	fn blob_column(&mut self, col: usize) -> Result<impl ColumnWriter<[u8]> + '_, SdkError>;
	fn decimal_column(&mut self, col: usize) -> Result<impl ColumnWriter<Decimal> + '_, SdkError>;
	fn dictionary_id_column(&mut self, col: usize) -> Result<impl ColumnWriter<DictionaryEntryId> + '_, SdkError>;
	fn push_none(&mut self, col: usize) -> Result<(), SdkError>;

	fn push_u8(&mut self, col: usize, v: u8) -> Result<(), SdkError> {
		self.u8_column(col)?.push(&v)
	}
	fn push_u16(&mut self, col: usize, v: u16) -> Result<(), SdkError> {
		self.u16_column(col)?.push(&v)
	}
	fn push_u32(&mut self, col: usize, v: u32) -> Result<(), SdkError> {
		self.u32_column(col)?.push(&v)
	}
	fn push_u64(&mut self, col: usize, v: u64) -> Result<(), SdkError> {
		self.u64_column(col)?.push(&v)
	}
	fn push_u128(&mut self, col: usize, v: u128) -> Result<(), SdkError> {
		self.u128_column(col)?.push(&v)
	}
	fn push_i8(&mut self, col: usize, v: i8) -> Result<(), SdkError> {
		self.i8_column(col)?.push(&v)
	}
	fn push_i16(&mut self, col: usize, v: i16) -> Result<(), SdkError> {
		self.i16_column(col)?.push(&v)
	}
	fn push_i32(&mut self, col: usize, v: i32) -> Result<(), SdkError> {
		self.i32_column(col)?.push(&v)
	}
	fn push_i64(&mut self, col: usize, v: i64) -> Result<(), SdkError> {
		self.i64_column(col)?.push(&v)
	}
	fn push_i128(&mut self, col: usize, v: i128) -> Result<(), SdkError> {
		self.i128_column(col)?.push(&v)
	}
	fn push_f32(&mut self, col: usize, v: f32) -> Result<(), SdkError> {
		self.f32_column(col)?.push(&v)
	}
	fn push_f64(&mut self, col: usize, v: f64) -> Result<(), SdkError> {
		self.f64_column(col)?.push(&v)
	}
	fn push_date(&mut self, col: usize, v: Date) -> Result<(), SdkError> {
		self.date_column(col)?.push(&v)
	}
	fn push_datetime(&mut self, col: usize, v: DateTime) -> Result<(), SdkError> {
		self.datetime_column(col)?.push(&v)
	}
	fn push_time(&mut self, col: usize, v: Time) -> Result<(), SdkError> {
		self.time_column(col)?.push(&v)
	}
	fn push_duration(&mut self, col: usize, v: Duration) -> Result<(), SdkError> {
		self.duration_column(col)?.push(&v)
	}
	fn push_bool(&mut self, col: usize, v: bool) -> Result<(), SdkError> {
		self.bool_column(col)?.push(&v)
	}
	fn push_utf8(&mut self, col: usize, v: &str) -> Result<(), SdkError> {
		self.utf8_column(col)?.push(v)
	}
	fn push_blob(&mut self, col: usize, v: &[u8]) -> Result<(), SdkError> {
		self.blob_column(col)?.push(v)
	}
	fn push_decimal(&mut self, col: usize, v: &Decimal) -> Result<(), SdkError> {
		self.decimal_column(col)?.push(v)
	}
}

pub(crate) struct NoneOnNan<W>(pub(crate) W);

impl<F: Copy + Into<f64>, W: ColumnWriter<F>> ColumnWriter<F> for NoneOnNan<W> {
	#[inline]
	fn push(&mut self, value: &F) -> Result<(), SdkError> {
		if (*value).into().is_nan() {
			self.0.push_none()
		} else {
			self.0.push(value)
		}
	}

	#[inline]
	fn push_none(&mut self) -> Result<(), SdkError> {
		self.0.push_none()
	}
}

pub(crate) fn column_mismatch(col: usize, kind: ValueKind, cell: &str) -> SdkError {
	SdkError::InvalidInput(format!("column {col} is {kind:?}, cannot push {cell}"))
}

pub(crate) fn unsupported_column(kind: ValueKind) -> SdkError {
	SdkError::NotImplemented(format!("a row sink does not support column type {kind:?}"))
}

pub(crate) fn column_type(kind: ValueKind) -> Result<ValueType, SdkError> {
	if !matches!(
		kind,
		ValueKind::Boolean
			| ValueKind::Uint1 | ValueKind::Uint2
			| ValueKind::Uint4 | ValueKind::Uint8
			| ValueKind::Uint16 | ValueKind::Int1
			| ValueKind::Int2 | ValueKind::Int4
			| ValueKind::Int8 | ValueKind::Int16
			| ValueKind::Float4 | ValueKind::Float8
			| ValueKind::Date | ValueKind::DateTime
			| ValueKind::Time | ValueKind::Duration
			| ValueKind::Utf8 | ValueKind::Blob
			| ValueKind::Decimal | ValueKind::DictionaryId
	) {
		return Err(unsupported_column(kind));
	}
	TypeTag::new(kind, 0)
		.map_err(|err| SdkError::Other(err.to_string()))?
		.to_type()
		.map_err(|err| SdkError::Other(err.to_string()))
}
