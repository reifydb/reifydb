// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

use crate::value::column::columns::Columns;

#[derive(Clone, Copy)]
pub struct RowRef<'a> {
	columns: &'a Columns,
	index: usize,
}

impl<'a> RowRef<'a> {
	pub(crate) fn new(columns: &'a Columns, index: usize) -> Self {
		Self {
			columns,
			index,
		}
	}

	pub fn value(&self, name: &str) -> Option<Value> {
		let col = self.columns.column(name)?;
		Some(col.data().get_value(self.index))
	}

	pub fn utf8(&self, name: &str) -> Option<String> {
		match self.value(name)? {
			Value::Utf8(s) => Some(s),
			_ => None,
		}
	}

	pub fn bool(&self, name: &str) -> Option<bool> {
		match self.value(name)? {
			Value::Boolean(b) => Some(b),
			_ => None,
		}
	}

	pub fn u64(&self, name: &str) -> Option<u64> {
		match self.value(name)? {
			Value::Uint8(v) => Some(v),
			Value::Uint4(v) => Some(v as u64),
			Value::Uint2(v) => Some(v as u64),
			Value::Uint1(v) => Some(v as u64),
			_ => None,
		}
	}

	pub fn u32(&self, name: &str) -> Option<u32> {
		match self.value(name)? {
			Value::Uint4(v) => Some(v),
			Value::Uint2(v) => Some(v as u32),
			Value::Uint1(v) => Some(v as u32),
			_ => None,
		}
	}

	pub fn u16(&self, name: &str) -> Option<u16> {
		match self.value(name)? {
			Value::Uint2(v) => Some(v),
			Value::Uint1(v) => Some(v as u16),
			_ => None,
		}
	}

	pub fn u8(&self, name: &str) -> Option<u8> {
		match self.value(name)? {
			Value::Uint1(v) => Some(v),
			_ => None,
		}
	}

	pub fn f64(&self, name: &str) -> Option<f64> {
		match self.value(name)? {
			Value::Float8(v) => Some(v.into()),
			Value::Float4(v) => Some(f32::from(v) as f64),
			_ => None,
		}
	}

	pub fn f32(&self, name: &str) -> Option<f32> {
		match self.value(name)? {
			Value::Float4(v) => Some(v.into()),
			_ => None,
		}
	}

	pub fn datetime(&self, name: &str) -> Option<DateTime> {
		match self.value(name)? {
			Value::DateTime(v) => Some(v),
			_ => None,
		}
	}

	pub fn duration(&self, name: &str) -> Option<Duration> {
		match self.value(name)? {
			Value::Duration(v) => Some(v),
			_ => None,
		}
	}
}

impl Columns {
	pub fn row_ref(&self, index: usize) -> Option<RowRef<'_>> {
		if index >= self.row_count() {
			return None;
		}
		Some(RowRef::new(self, index))
	}
}
