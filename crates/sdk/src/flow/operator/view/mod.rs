// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod in_process;

use reifydb_value::value::{
	Value, column_view::ColumnView, date::Date, datetime::DateTime, decimal::Decimal, diff_type::DiffType,
	duration::Duration, row_number::RowNumber, system_columns::RowTimes, time::Time,
};

use crate::{error::SdkError, flow::operator::change::BorrowedColumn};

pub trait ViewCell<'a>: Sized {
	fn read_view(view: &ColumnView<'a>, index: usize) -> Result<Option<Self>, SdkError>;

	fn read_value(value: &'a Value, view: &ColumnView<'_>) -> Result<Option<Self>, SdkError>;
}

pub trait BorrowedCell<'a>: Sized {
	fn read_borrowed(column: &BorrowedColumn<'a>, index: usize) -> Result<Option<Self>, SdkError>;

	fn read_value(value: &'a Value, column: &BorrowedColumn<'_>) -> Result<Option<Self>, SdkError>;
}

pub trait CursorValue<'a>: ViewCell<'a> + BorrowedCell<'a> {}

impl<'a, T: ViewCell<'a> + BorrowedCell<'a>> CursorValue<'a> for T {}

pub trait ColumnCursor<T> {
	fn get(&self, index: usize) -> Result<Option<T>, SdkError>;
}

pub trait RowView {
	fn is_defined(&self, name: &str) -> Result<bool, SdkError>;
	fn utf8(&self, name: &str) -> Result<Option<&str>, SdkError>;
	fn blob(&self, name: &str) -> Result<Option<&[u8]>, SdkError>;
	fn bool(&self, name: &str) -> Result<Option<bool>, SdkError>;
	fn u8(&self, name: &str) -> Result<Option<u8>, SdkError>;
	fn u16(&self, name: &str) -> Result<Option<u16>, SdkError>;
	fn u32(&self, name: &str) -> Result<Option<u32>, SdkError>;
	fn u64(&self, name: &str) -> Result<Option<u64>, SdkError>;
	fn u128(&self, name: &str) -> Result<Option<u128>, SdkError>;
	fn i8(&self, name: &str) -> Result<Option<i8>, SdkError>;
	fn i16(&self, name: &str) -> Result<Option<i16>, SdkError>;
	fn i32(&self, name: &str) -> Result<Option<i32>, SdkError>;
	fn i64(&self, name: &str) -> Result<Option<i64>, SdkError>;
	fn i128(&self, name: &str) -> Result<Option<i128>, SdkError>;
	fn f32(&self, name: &str) -> Result<Option<f32>, SdkError>;
	fn f64(&self, name: &str) -> Result<Option<f64>, SdkError>;
	fn decimal(&self, name: &str) -> Result<Option<Decimal>, SdkError>;
	fn date(&self, name: &str) -> Result<Option<Date>, SdkError>;
	fn datetime(&self, name: &str) -> Result<Option<DateTime>, SdkError>;
	fn time(&self, name: &str) -> Result<Option<Time>, SdkError>;
	fn duration(&self, name: &str) -> Result<Option<Duration>, SdkError>;
	fn row_number(&self) -> Option<RowNumber>;
	fn row_time(&self) -> Option<DateTime>;
}

pub trait ColumnsView {
	fn row_count(&self) -> usize;
	fn row(&self, index: usize) -> Option<impl RowView + '_>;
	fn column<'c, T: CursorValue<'c> + 'c>(
		&'c self,
		name: &str,
	) -> Result<Option<impl ColumnCursor<T> + 'c>, SdkError>;
	fn row_numbers(&self) -> &[RowNumber];
	fn times(&self) -> RowTimes<'_>;
}

pub trait DiffView {
	fn kind(&self) -> DiffType;
	fn pre(&self) -> Option<impl ColumnsView + '_>;
	fn post(&self) -> Option<impl ColumnsView + '_>;
}

pub trait ChangeView {
	fn version(&self) -> u64;
	fn changed_at_nanos(&self) -> i64;
	fn diff_count(&self) -> usize;
	fn diff(&self, index: usize) -> Option<impl DiffView + '_>;
}
