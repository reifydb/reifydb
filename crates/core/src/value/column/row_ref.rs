// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	Value, date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, row_number::RowNumber, time::Time,
};

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

	pub fn index(&self) -> usize {
		self.index
	}

	pub fn row_number(&self) -> Option<RowNumber> {
		self.columns.row_numbers.get(self.index).copied()
	}

	pub fn value(&self, name: &str) -> Option<Value> {
		let col = self.columns.column(name)?;
		Some(col.data().get_value(self.index))
	}

	pub fn is_defined(&self, name: &str) -> bool {
		match self.value(name) {
			Some(Value::None {
				..
			}) => false,
			Some(_) => true,
			None => false,
		}
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

	pub fn i64(&self, name: &str) -> Option<i64> {
		match self.value(name)? {
			Value::Int8(v) => Some(v),
			Value::Int4(v) => Some(v as i64),
			Value::Int2(v) => Some(v as i64),
			Value::Int1(v) => Some(v as i64),
			_ => None,
		}
	}

	pub fn i32(&self, name: &str) -> Option<i32> {
		match self.value(name)? {
			Value::Int4(v) => Some(v),
			Value::Int2(v) => Some(v as i32),
			Value::Int1(v) => Some(v as i32),
			_ => None,
		}
	}

	pub fn i16(&self, name: &str) -> Option<i16> {
		match self.value(name)? {
			Value::Int2(v) => Some(v),
			Value::Int1(v) => Some(v as i16),
			_ => None,
		}
	}

	pub fn i8(&self, name: &str) -> Option<i8> {
		match self.value(name)? {
			Value::Int1(v) => Some(v),
			_ => None,
		}
	}

	pub fn u128(&self, name: &str) -> Option<u128> {
		match self.value(name)? {
			Value::Uint16(v) => Some(v),
			_ => None,
		}
	}

	pub fn i128(&self, name: &str) -> Option<i128> {
		match self.value(name)? {
			Value::Int16(v) => Some(v),
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

	pub fn decimal(&self, name: &str) -> Option<Decimal> {
		match self.value(name)? {
			Value::Decimal(v) => Some(v),
			Value::Float8(v) => Some(Decimal::from(f64::from(v))),
			Value::Float4(v) => Some(Decimal::from(f32::from(v) as f64)),
			_ => None,
		}
	}

	pub fn blob(&self, name: &str) -> Option<Vec<u8>> {
		match self.value(name)? {
			Value::Blob(b) => Some(b.as_bytes().to_vec()),
			_ => None,
		}
	}

	pub fn date(&self, name: &str) -> Option<Date> {
		match self.value(name)? {
			Value::Date(v) => Some(v),
			_ => None,
		}
	}

	pub fn datetime(&self, name: &str) -> Option<DateTime> {
		match self.value(name)? {
			Value::DateTime(v) => Some(v),
			_ => None,
		}
	}

	pub fn time(&self, name: &str) -> Option<Time> {
		match self.value(name)? {
			Value::Time(v) => Some(v),
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

pub struct RowRefIter<'a> {
	columns: &'a Columns,
	index: usize,
	end: usize,
}

impl<'a> Iterator for RowRefIter<'a> {
	type Item = RowRef<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.index >= self.end {
			return None;
		}
		let r = RowRef::new(self.columns, self.index);
		self.index += 1;
		Some(r)
	}
}

impl Columns {
	pub fn row_ref(&self, index: usize) -> Option<RowRef<'_>> {
		if index >= self.row_count() {
			return None;
		}
		Some(RowRef::new(self, index))
	}

	pub fn row_refs(&self) -> RowRefIter<'_> {
		RowRefIter {
			columns: self,
			index: 0,
			end: self.row_count(),
		}
	}
}
