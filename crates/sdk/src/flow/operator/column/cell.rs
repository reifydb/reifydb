// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::tag::ValueKind;
use reifydb_value::value::{
	date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, int::Int, time::Time, uint::Uint,
};

use crate::{error::SdkError, flow::operator::column::sink::RowSink};

pub trait Cell: Sized {
	const COLUMN_TYPE: ValueKind;
	const AVG_BYTES: usize = 0;

	fn encode<S: RowSink>(&self, sink: &mut S, col: usize) -> Result<(), SdkError>;
}

macro_rules! impl_cell_scalar {
	($ty:ty, $code:expr, $push:ident) => {
		impl Cell for $ty {
			const COLUMN_TYPE: ValueKind = $code;
			#[inline]
			fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
				e.$push(col, *self);
				Ok(())
			}
		}
	};
}

impl_cell_scalar!(u8, ValueKind::Uint1, push_u8);
impl_cell_scalar!(u16, ValueKind::Uint2, push_u16);
impl_cell_scalar!(u32, ValueKind::Uint4, push_u32);
impl_cell_scalar!(u64, ValueKind::Uint8, push_u64);
impl_cell_scalar!(i8, ValueKind::Int1, push_i8);
impl_cell_scalar!(i16, ValueKind::Int2, push_i16);
impl_cell_scalar!(i32, ValueKind::Int4, push_i32);
impl_cell_scalar!(i64, ValueKind::Int8, push_i64);
impl_cell_scalar!(f32, ValueKind::Float4, push_f32);
impl_cell_scalar!(f64, ValueKind::Float8, push_f64);
impl_cell_scalar!(bool, ValueKind::Boolean, push_bool);

impl Cell for u128 {
	const COLUMN_TYPE: ValueKind = ValueKind::Uint16;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_u128(col, *self);
		Ok(())
	}
}

impl Cell for i128 {
	const COLUMN_TYPE: ValueKind = ValueKind::Int16;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_i128(col, *self);
		Ok(())
	}
}

impl Cell for String {
	const COLUMN_TYPE: ValueKind = ValueKind::Utf8;
	const AVG_BYTES: usize = 24;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_utf8(col, self.as_str())
	}
}

impl Cell for Arc<str> {
	const COLUMN_TYPE: ValueKind = ValueKind::Utf8;
	const AVG_BYTES: usize = 24;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_utf8(col, self.as_ref())
	}
}

impl Cell for Vec<u8> {
	const COLUMN_TYPE: ValueKind = ValueKind::Blob;
	const AVG_BYTES: usize = 32;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_blob(col, self.as_slice())
	}
}

impl Cell for Int {
	const COLUMN_TYPE: ValueKind = ValueKind::Int;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_int(col, self)
	}
}

impl Cell for Uint {
	const COLUMN_TYPE: ValueKind = ValueKind::Uint;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_uint(col, self)
	}
}

impl Cell for Decimal {
	const COLUMN_TYPE: ValueKind = ValueKind::Decimal;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_decimal(col, self)
	}
}

impl Cell for Date {
	const COLUMN_TYPE: ValueKind = ValueKind::Date;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_date(col, *self);
		Ok(())
	}
}

impl Cell for DateTime {
	const COLUMN_TYPE: ValueKind = ValueKind::DateTime;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_datetime(col, *self);
		Ok(())
	}
}

impl Cell for Time {
	const COLUMN_TYPE: ValueKind = ValueKind::Time;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_time(col, *self);
		Ok(())
	}
}

impl Cell for Duration {
	const COLUMN_TYPE: ValueKind = ValueKind::Duration;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		e.push_duration(col, *self);
		Ok(())
	}
}

impl<T: Cell> Cell for Option<T> {
	const COLUMN_TYPE: ValueKind = T::COLUMN_TYPE;
	const AVG_BYTES: usize = T::AVG_BYTES;
	#[inline]
	fn encode<S: RowSink>(&self, e: &mut S, col: usize) -> Result<(), SdkError> {
		match self {
			Some(v) => v.encode(e, col),
			None => e.push_none(col),
		}
	}
}
