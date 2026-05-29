// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use postcard::to_allocvec;
use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_value::value::{date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, time::Time};

use crate::{
	error::SdkError,
	operator::{column::sink::RowSink, view::RowView},
};

pub trait Cell: Sized {
	const COLUMN_TYPE: ColumnTypeCode;
	const AVG_BYTES: usize = 0;

	fn encode<S: RowSink>(&self, sink: &mut S, col: usize) -> Result<(), SdkError>;
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self>;
}

macro_rules! impl_cell_scalar {
	($ty:ty, $code:expr, $push:ident, $read:ident) => {
		impl Cell for $ty {
			const COLUMN_TYPE: ColumnTypeCode = $code;
			#[inline]
			fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
				e.$push(col, *self);
				Ok(())
			}
			#[inline]
			fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
				view.$read(name)
			}
		}
	};
}

impl_cell_scalar!(u8, ColumnTypeCode::Uint1, push_u8, u8);
impl_cell_scalar!(u16, ColumnTypeCode::Uint2, push_u16, u16);
impl_cell_scalar!(u32, ColumnTypeCode::Uint4, push_u32, u32);
impl_cell_scalar!(u64, ColumnTypeCode::Uint8, push_u64, u64);
impl_cell_scalar!(i8, ColumnTypeCode::Int1, push_i8, i8);
impl_cell_scalar!(i16, ColumnTypeCode::Int2, push_i16, i16);
impl_cell_scalar!(i32, ColumnTypeCode::Int4, push_i32, i32);
impl_cell_scalar!(i64, ColumnTypeCode::Int8, push_i64, i64);
impl_cell_scalar!(f32, ColumnTypeCode::Float4, push_f32, f32);
impl_cell_scalar!(f64, ColumnTypeCode::Float8, push_f64, f64);
impl_cell_scalar!(bool, ColumnTypeCode::Bool, push_bool, bool);

impl Cell for u128 {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Uint16;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_u128(col, *self);
		Ok(())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.u128(name)
	}
}

impl Cell for i128 {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Int16;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_i128(col, *self);
		Ok(())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.i128(name)
	}
}

impl Cell for String {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Utf8;
	const AVG_BYTES: usize = 24;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_utf8(col, self.as_str())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.utf8(name).map(str::to_string)
	}
}

impl Cell for Arc<str> {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Utf8;
	const AVG_BYTES: usize = 24;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_utf8(col, self.as_ref())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.utf8(name).map(Arc::from)
	}
}

impl Cell for Vec<u8> {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Blob;
	const AVG_BYTES: usize = 32;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_blob(col, self.as_slice())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.blob(name).map(<[u8]>::to_vec)
	}
}

impl Cell for Decimal {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Decimal;
	const AVG_BYTES: usize = 16;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		let bytes = to_allocvec(self)
			.map_err(|err| SdkError::Serialization(format!("decimal serialize: {}", err)))?;
		e.push_decimal_bytes(col, &bytes)
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.decimal(name)
	}
}

impl Cell for Date {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Date;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_date(col, *self);
		Ok(())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.date(name)
	}
}

impl Cell for DateTime {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::DateTime;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_datetime(col, *self);
		Ok(())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.datetime(name)
	}
}

impl Cell for Time {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Time;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_time(col, *self);
		Ok(())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.time(name)
	}
}

impl Cell for Duration {
	const COLUMN_TYPE: ColumnTypeCode = ColumnTypeCode::Duration;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_duration(col, *self);
		Ok(())
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		view.duration(name)
	}
}

impl<T: Cell> Cell for Option<T> {
	const COLUMN_TYPE: ColumnTypeCode = T::COLUMN_TYPE;
	const AVG_BYTES: usize = T::AVG_BYTES;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		match self {
			Some(v) => v.encode(e, col),
			None => e.push_none(col),
		}
	}
	#[inline]
	fn decode<V: RowView>(view: &V, name: &str) -> Option<Self> {
		Some(if view.is_defined(name) {
			T::decode(view, name)
		} else {
			None
		})
	}
}
