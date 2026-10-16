// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::once, sync::Arc};

use reifydb_codec::tag::ValueKind;
use reifydb_value::value::{
	date::Date, datetime::DateTime, decimal::Decimal, dictionary::DictionaryEntryId, duration::Duration, time::Time,
};

use crate::{
	error::SdkError,
	flow::operator::column::sink::{ColumnWriter, RowSink},
};

pub trait Cell: Sized {
	const COLUMN_TYPE: ValueKind;
	const AVG_BYTES: usize = 0;

	fn encode_column<'r, S: RowSink>(
		sink: &mut S,
		col: usize,
		cells: impl Iterator<Item = Option<&'r Self>>,
	) -> Result<(), SdkError>
	where
		Self: 'r;

	#[inline]
	fn encode<S: RowSink>(&self, sink: &mut S, col: usize) -> Result<(), SdkError> {
		Self::encode_column(sink, col, once(Some(self)))
	}
}

#[inline]
fn write_cells<'r, C: 'r, T: ?Sized>(
	mut writer: impl ColumnWriter<T>,
	cells: impl Iterator<Item = Option<&'r C>>,
	view: impl Fn(&C) -> &T,
) -> Result<(), SdkError> {
	for cell in cells {
		match cell {
			Some(value) => writer.push(view(value))?,
			None => writer.push_none()?,
		}
	}
	Ok(())
}

macro_rules! impl_cell {
	($ty:ty, $kind:expr, $avg:expr, $column:ident, $view:expr) => {
		impl Cell for $ty {
			const COLUMN_TYPE: ValueKind = $kind;
			const AVG_BYTES: usize = $avg;
			#[inline]
			fn encode_column<'r, S: RowSink>(
				sink: &mut S,
				col: usize,
				cells: impl Iterator<Item = Option<&'r Self>>,
			) -> Result<(), SdkError>
			where
				Self: 'r,
			{
				write_cells(sink.$column(col)?, cells, $view)
			}
		}
	};
}

impl_cell!(u8, ValueKind::Uint1, 0, u8_column, |v| v);
impl_cell!(u16, ValueKind::Uint2, 0, u16_column, |v| v);
impl_cell!(u32, ValueKind::Uint4, 0, u32_column, |v| v);
impl_cell!(u64, ValueKind::Uint8, 0, u64_column, |v| v);
impl_cell!(u128, ValueKind::Uint16, 0, u128_column, |v| v);
impl_cell!(i8, ValueKind::Int1, 0, i8_column, |v| v);
impl_cell!(i16, ValueKind::Int2, 0, i16_column, |v| v);
impl_cell!(i32, ValueKind::Int4, 0, i32_column, |v| v);
impl_cell!(i64, ValueKind::Int8, 0, i64_column, |v| v);
impl_cell!(i128, ValueKind::Int16, 0, i128_column, |v| v);
impl_cell!(f32, ValueKind::Float4, 0, f32_column, |v| v);
impl_cell!(f64, ValueKind::Float8, 0, f64_column, |v| v);
impl_cell!(bool, ValueKind::Boolean, 0, bool_column, |v| v);
impl_cell!(String, ValueKind::Utf8, 24, utf8_column, String::as_str);
impl_cell!(Arc<str>, ValueKind::Utf8, 24, utf8_column, |v: &Arc<str>| v.as_ref());
impl_cell!(Vec<u8>, ValueKind::Blob, 32, blob_column, Vec::as_slice);
impl_cell!(Decimal, ValueKind::Decimal, 0, decimal_column, |v| v);
impl_cell!(Date, ValueKind::Date, 0, date_column, |v| v);
impl_cell!(DateTime, ValueKind::DateTime, 0, datetime_column, |v| v);
impl_cell!(Time, ValueKind::Time, 0, time_column, |v| v);
impl_cell!(Duration, ValueKind::Duration, 0, duration_column, |v| v);
impl_cell!(DictionaryEntryId, ValueKind::DictionaryId, 0, dictionary_id_column, |v| v);

impl<T: Cell> Cell for Option<T> {
	const COLUMN_TYPE: ValueKind = T::COLUMN_TYPE;
	const AVG_BYTES: usize = T::AVG_BYTES;
	#[inline]
	fn encode_column<'r, S: RowSink>(
		sink: &mut S,
		col: usize,
		cells: impl Iterator<Item = Option<&'r Self>>,
	) -> Result<(), SdkError>
	where
		Self: 'r,
	{
		T::encode_column(sink, col, cells.map(|cell| cell.and_then(Option::as_ref)))
	}
}
