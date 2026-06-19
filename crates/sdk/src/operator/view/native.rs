// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{
	interface::change::{Change, Diff},
	value::column::{
		buffer::{ColumnBuffer, get::FromColumnBuffer},
		columns::Columns,
	},
};
use reifydb_value::value::{
	Value, date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, row_number::RowNumber, time::Time,
};

use super::{ChangeView, ColumnsView, DiffView, RowView};

pub struct NativeRowView<'a> {
	columns: &'a Columns,
	index: usize,
}

impl<'a> NativeRowView<'a> {
	pub fn new(columns: &'a Columns, index: usize) -> Self {
		Self {
			columns,
			index,
		}
	}

	fn buffer(&self, name: &str) -> Option<&'a ColumnBuffer> {
		self.columns.column(name).map(|c| c.data())
	}

	fn defined_inner(&self, name: &str) -> Option<&'a ColumnBuffer> {
		let buffer = self.buffer(name)?;
		if !buffer.is_defined(self.index) {
			return None;
		}
		Some(buffer.unwrap_option().0)
	}

	fn typed<T: FromColumnBuffer>(&self, name: &str) -> Option<T> {
		self.defined_inner(name)?.get_as::<T>(self.index)
	}
}

impl<'a> RowView for NativeRowView<'a> {
	fn is_defined(&self, name: &str) -> bool {
		self.buffer(name).map(|b| b.is_defined(self.index)).unwrap_or(false)
	}

	fn utf8(&self, name: &str) -> Option<&str> {
		self.defined_inner(name)?.get_str(self.index)
	}

	fn blob(&self, name: &str) -> Option<&[u8]> {
		self.defined_inner(name)?.get_bytes(self.index)
	}

	fn bool(&self, name: &str) -> Option<bool> {
		self.typed(name)
	}

	fn u8(&self, name: &str) -> Option<u8> {
		self.typed(name)
	}

	fn u16(&self, name: &str) -> Option<u16> {
		self.typed(name)
	}

	fn u32(&self, name: &str) -> Option<u32> {
		self.typed(name)
	}

	fn u64(&self, name: &str) -> Option<u64> {
		self.typed(name)
	}

	fn u128(&self, name: &str) -> Option<u128> {
		self.typed(name)
	}

	fn i8(&self, name: &str) -> Option<i8> {
		self.typed(name)
	}

	fn i16(&self, name: &str) -> Option<i16> {
		self.typed(name)
	}

	fn i32(&self, name: &str) -> Option<i32> {
		self.typed(name)
	}

	fn i64(&self, name: &str) -> Option<i64> {
		self.typed(name)
	}

	fn i128(&self, name: &str) -> Option<i128> {
		self.typed(name)
	}

	fn f32(&self, name: &str) -> Option<f32> {
		self.typed(name)
	}

	fn f64(&self, name: &str) -> Option<f64> {
		self.typed(name)
	}

	fn decimal(&self, name: &str) -> Option<Decimal> {
		self.typed(name)
	}

	fn date(&self, name: &str) -> Option<Date> {
		self.typed(name)
	}

	fn datetime(&self, name: &str) -> Option<DateTime> {
		self.typed(name)
	}

	fn time(&self, name: &str) -> Option<Time> {
		self.typed(name)
	}

	fn duration(&self, name: &str) -> Option<Duration> {
		self.typed(name)
	}

	fn value(&self, name: &str) -> Option<Value> {
		self.buffer(name).map(|b| b.get_value(self.index))
	}

	fn row_number(&self) -> Option<RowNumber> {
		self.columns.row_numbers.get(self.index).copied()
	}

	fn created_at_nanos(&self) -> Option<u64> {
		self.columns.created_at.get(self.index).map(DateTime::to_nanos)
	}

	fn updated_at_nanos(&self) -> Option<u64> {
		self.columns.updated_at.get(self.index).map(DateTime::to_nanos)
	}
}

pub struct NativeColumnsView<'a> {
	columns: &'a Columns,
}

impl<'a> NativeColumnsView<'a> {
	pub fn new(columns: &'a Columns) -> Self {
		Self {
			columns,
		}
	}
}

impl<'a> ColumnsView for NativeColumnsView<'a> {
	fn row_count(&self) -> usize {
		self.columns.row_count()
	}

	fn row(&self, index: usize) -> Option<impl RowView + '_> {
		if index >= self.columns.row_count() {
			return None;
		}
		Some(NativeRowView::new(self.columns, index))
	}
}

pub struct NativeDiffView<'a> {
	diff: &'a Diff,
}

impl<'a> NativeDiffView<'a> {
	pub fn new(diff: &'a Diff) -> Self {
		Self {
			diff,
		}
	}
}

impl<'a> DiffView for NativeDiffView<'a> {
	fn kind(&self) -> DiffType {
		self.diff.kind()
	}

	fn pre(&self) -> Option<impl ColumnsView + '_> {
		self.diff.pre().map(NativeColumnsView::new)
	}

	fn post(&self) -> Option<impl ColumnsView + '_> {
		self.diff.post().map(NativeColumnsView::new)
	}
}

pub struct NativeChangeView<'a> {
	change: &'a Change,
}

impl<'a> NativeChangeView<'a> {
	pub fn new(change: &'a Change) -> Self {
		Self {
			change,
		}
	}
}

impl<'a> ChangeView for NativeChangeView<'a> {
	fn version(&self) -> u64 {
		self.change.version.0
	}

	fn changed_at_nanos(&self) -> u64 {
		self.change.changed_at.to_nanos()
	}

	fn diff_count(&self) -> usize {
		self.change.diffs.len()
	}

	fn diff(&self, index: usize) -> Option<impl DiffView + '_> {
		self.change.diffs.get(index).map(NativeDiffView::new)
	}
}
