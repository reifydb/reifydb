// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod in_process;

use reifydb_value::value::{
	Value, date::Date, datetime::DateTime, decimal::Decimal, diff_type::DiffType, duration::Duration, int::Int,
	row_number::RowNumber, time::Time, uint::Uint,
};

use crate::error::SdkError;

pub trait RowView {
	fn is_defined(&self, name: &str) -> bool;
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
	fn int(&self, name: &str) -> Result<Option<Int>, SdkError>;
	fn uint(&self, name: &str) -> Result<Option<Uint>, SdkError>;
	fn decimal(&self, name: &str) -> Result<Option<Decimal>, SdkError>;
	fn date(&self, name: &str) -> Result<Option<Date>, SdkError>;
	fn datetime(&self, name: &str) -> Result<Option<DateTime>, SdkError>;
	fn time(&self, name: &str) -> Result<Option<Time>, SdkError>;
	fn duration(&self, name: &str) -> Result<Option<Duration>, SdkError>;
	fn value(&self, name: &str) -> Option<Value>;
	fn row_number(&self) -> Option<RowNumber>;
	fn row_time(&self) -> Option<DateTime>;
}

pub trait ColumnsView {
	fn row_count(&self) -> usize;
	fn row(&self, index: usize) -> Option<impl RowView + '_>;
}

pub trait DiffView {
	fn kind(&self) -> DiffType;
	fn pre(&self) -> Option<impl ColumnsView + '_>;
	fn post(&self) -> Option<impl ColumnsView + '_>;
}

pub trait ChangeView {
	fn version(&self) -> u64;
	fn changed_at_nanos(&self) -> u64;
	fn diff_count(&self) -> usize;
	fn diff(&self, index: usize) -> Option<impl DiffView + '_>;
}
