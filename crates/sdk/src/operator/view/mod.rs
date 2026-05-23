// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod ffi;
pub mod native;

use reifydb_abi::flow::diff::DiffType;
use reifydb_type::value::{
	Value, date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, row_number::RowNumber, time::Time,
};

pub trait RowView {
	fn is_defined(&self, name: &str) -> bool;
	fn utf8(&self, name: &str) -> Option<&str>;
	fn blob(&self, name: &str) -> Option<&[u8]>;
	fn bool(&self, name: &str) -> Option<bool>;
	fn u8(&self, name: &str) -> Option<u8>;
	fn u16(&self, name: &str) -> Option<u16>;
	fn u32(&self, name: &str) -> Option<u32>;
	fn u64(&self, name: &str) -> Option<u64>;
	fn u128(&self, name: &str) -> Option<u128>;
	fn i8(&self, name: &str) -> Option<i8>;
	fn i16(&self, name: &str) -> Option<i16>;
	fn i32(&self, name: &str) -> Option<i32>;
	fn i64(&self, name: &str) -> Option<i64>;
	fn i128(&self, name: &str) -> Option<i128>;
	fn f32(&self, name: &str) -> Option<f32>;
	fn f64(&self, name: &str) -> Option<f64>;
	fn decimal(&self, name: &str) -> Option<Decimal>;
	fn date(&self, name: &str) -> Option<Date>;
	fn datetime(&self, name: &str) -> Option<DateTime>;
	fn time(&self, name: &str) -> Option<Time>;
	fn duration(&self, name: &str) -> Option<Duration>;
	fn value(&self, name: &str) -> Option<Value>;
	fn row_number(&self) -> Option<RowNumber>;
	fn created_at_nanos(&self) -> Option<u64>;
	fn updated_at_nanos(&self) -> Option<u64>;
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
