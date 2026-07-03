// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::{data::column::ColumnTypeCode, flow::diff::DiffType};
use reifydb_codec::ffi::cells::decode_decimal_cell;
use reifydb_value::value::{
	Value, date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, ordered_f32::OrderedF32,
	ordered_f64::OrderedF64, row_number::RowNumber, time::Time, value_type::ValueType,
};

use crate::operator::{
	change::{BorrowedChange, BorrowedColumn, BorrowedColumns, BorrowedDiff},
	view::{ChangeView, ColumnsView, DiffView, RowView},
};

#[derive(Clone, Copy)]
pub struct FFIRowView<'a> {
	columns: BorrowedColumns<'a>,
	index: usize,
}

impl<'a> FFIRowView<'a> {
	pub(crate) fn new(columns: BorrowedColumns<'a>, index: usize) -> Self {
		Self {
			columns,
			index,
		}
	}

	pub fn index(&self) -> usize {
		self.index
	}

	pub fn columns(&self) -> BorrowedColumns<'a> {
		self.columns
	}

	fn column_defined(&self, name: &str) -> Option<BorrowedColumn<'a>> {
		let col = self.columns.column(name)?;
		if !is_defined_at(&col, self.index) {
			return None;
		}
		Some(col)
	}
}

impl<'a> RowView for FFIRowView<'a> {
	fn is_defined(&self, name: &str) -> bool {
		match self.columns.column(name) {
			Some(col) => is_defined_at(&col, self.index),
			None => false,
		}
	}

	fn utf8(&self, name: &str) -> Option<&str> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Utf8 {
			return None;
		}
		col.iter_str().nth(self.index)
	}

	fn blob(&self, name: &str) -> Option<&[u8]> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Blob {
			return None;
		}
		col.iter_bytes().nth(self.index)
	}

	fn bool(&self, name: &str) -> Option<bool> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Bool {
			return None;
		}
		let bytes = col.data_bytes();
		let byte = bytes.get(self.index / 8).copied()?;
		Some((byte >> (self.index % 8)) & 1 == 1)
	}

	fn u64(&self, name: &str) -> Option<u64> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Uint8 => fixed_at::<u64>(&col, self.index),
			ColumnTypeCode::Uint4 => fixed_at::<u32>(&col, self.index).map(u64::from),
			ColumnTypeCode::Uint2 => fixed_at::<u16>(&col, self.index).map(u64::from),
			ColumnTypeCode::Uint1 => fixed_at::<u8>(&col, self.index).map(u64::from),
			_ => None,
		}
	}

	fn u32(&self, name: &str) -> Option<u32> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Uint4 => fixed_at::<u32>(&col, self.index),
			ColumnTypeCode::Uint2 => fixed_at::<u16>(&col, self.index).map(u32::from),
			ColumnTypeCode::Uint1 => fixed_at::<u8>(&col, self.index).map(u32::from),
			_ => None,
		}
	}

	fn u16(&self, name: &str) -> Option<u16> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Uint2 => fixed_at::<u16>(&col, self.index),
			ColumnTypeCode::Uint1 => fixed_at::<u8>(&col, self.index).map(u16::from),
			_ => None,
		}
	}

	fn u8(&self, name: &str) -> Option<u8> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Uint1 {
			return None;
		}
		fixed_at::<u8>(&col, self.index)
	}

	fn i64(&self, name: &str) -> Option<i64> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Int8 => fixed_at::<i64>(&col, self.index),
			ColumnTypeCode::Int4 => fixed_at::<i32>(&col, self.index).map(i64::from),
			ColumnTypeCode::Int2 => fixed_at::<i16>(&col, self.index).map(i64::from),
			ColumnTypeCode::Int1 => fixed_at::<i8>(&col, self.index).map(i64::from),
			_ => None,
		}
	}

	fn i32(&self, name: &str) -> Option<i32> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Int4 => fixed_at::<i32>(&col, self.index),
			ColumnTypeCode::Int2 => fixed_at::<i16>(&col, self.index).map(i32::from),
			ColumnTypeCode::Int1 => fixed_at::<i8>(&col, self.index).map(i32::from),
			_ => None,
		}
	}

	fn i16(&self, name: &str) -> Option<i16> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Int2 => fixed_at::<i16>(&col, self.index),
			ColumnTypeCode::Int1 => fixed_at::<i8>(&col, self.index).map(i16::from),
			_ => None,
		}
	}

	fn i8(&self, name: &str) -> Option<i8> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Int1 {
			return None;
		}
		fixed_at::<i8>(&col, self.index)
	}

	fn u128(&self, name: &str) -> Option<u128> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Uint16 {
			return None;
		}
		fixed_at::<u128>(&col, self.index)
	}

	fn i128(&self, name: &str) -> Option<i128> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Int16 {
			return None;
		}
		fixed_at::<i128>(&col, self.index)
	}

	fn f64(&self, name: &str) -> Option<f64> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Float8 => fixed_at::<f64>(&col, self.index),
			ColumnTypeCode::Float4 => fixed_at::<f32>(&col, self.index).map(f64::from),
			_ => None,
		}
	}

	fn f32(&self, name: &str) -> Option<f32> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Float4 {
			return None;
		}
		fixed_at::<f32>(&col, self.index)
	}

	fn decimal(&self, name: &str) -> Option<Decimal> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Decimal => decode_decimal_at(&col, self.index),
			ColumnTypeCode::Float8 => fixed_at::<f64>(&col, self.index).map(Decimal::from),
			ColumnTypeCode::Float4 => fixed_at::<f32>(&col, self.index).map(|v| Decimal::from(v as f64)),
			_ => None,
		}
	}

	fn date(&self, name: &str) -> Option<Date> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Date {
			return None;
		}
		fixed_at::<Date>(&col, self.index)
	}

	fn datetime(&self, name: &str) -> Option<DateTime> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::DateTime {
			return None;
		}
		fixed_at::<DateTime>(&col, self.index)
	}

	fn time(&self, name: &str) -> Option<Time> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Time {
			return None;
		}
		fixed_at::<Time>(&col, self.index)
	}

	fn duration(&self, name: &str) -> Option<Duration> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Duration {
			return None;
		}
		fixed_at::<Duration>(&col, self.index)
	}

	fn value(&self, name: &str) -> Option<Value> {
		let col = self.columns.column(name)?;
		Some(read_value_at(&col, self.index))
	}

	fn row_number(&self) -> Option<RowNumber> {
		self.columns.row_numbers().get(self.index).copied().map(RowNumber)
	}

	fn created_at_nanos(&self) -> Option<u64> {
		self.columns.created_at().get(self.index).copied()
	}

	fn updated_at_nanos(&self) -> Option<u64> {
		self.columns.updated_at().get(self.index).copied()
	}
}

pub(crate) fn is_defined_at(col: &BorrowedColumn<'_>, index: usize) -> bool {
	let bv = col.defined_bitvec();
	if bv.is_empty() {
		return true;
	}
	let byte = match bv.get(index / 8) {
		Some(b) => *b,
		None => return false,
	};
	(byte >> (index % 8)) & 1 == 1
}

pub(crate) fn fixed_at<T: Copy>(col: &BorrowedColumn<'_>, index: usize) -> Option<T> {
	let slice = unsafe { col.as_slice::<T>()? };
	slice.get(index).copied()
}

pub(crate) fn decode_decimal_at(col: &BorrowedColumn<'_>, index: usize) -> Option<Decimal> {
	let data = col.data_bytes();
	let offsets = col.offsets();
	if index + 1 >= offsets.len() {
		return None;
	}
	let start = offsets[index] as usize;
	let end = offsets[index + 1] as usize;
	if end > data.len() || start > end {
		return None;
	}
	decode_decimal_cell(&data[start..end]).ok()
}

fn type_for_code(code: ColumnTypeCode) -> ValueType {
	match code {
		ColumnTypeCode::Bool => ValueType::Boolean,
		ColumnTypeCode::Float4 => ValueType::Float4,
		ColumnTypeCode::Float8 => ValueType::Float8,
		ColumnTypeCode::Int1 => ValueType::Int1,
		ColumnTypeCode::Int2 => ValueType::Int2,
		ColumnTypeCode::Int4 => ValueType::Int4,
		ColumnTypeCode::Int8 => ValueType::Int8,
		ColumnTypeCode::Int16 => ValueType::Int16,
		ColumnTypeCode::Uint1 => ValueType::Uint1,
		ColumnTypeCode::Uint2 => ValueType::Uint2,
		ColumnTypeCode::Uint4 => ValueType::Uint4,
		ColumnTypeCode::Uint8 => ValueType::Uint8,
		ColumnTypeCode::Uint16 => ValueType::Uint16,
		ColumnTypeCode::Utf8 => ValueType::Utf8,
		ColumnTypeCode::Decimal => ValueType::Decimal,
		ColumnTypeCode::Blob => ValueType::Blob,
		_ => ValueType::Any,
	}
}

fn none_value(code: ColumnTypeCode) -> Value {
	Value::None {
		inner: type_for_code(code),
	}
}

fn read_value_at(col: &BorrowedColumn<'_>, index: usize) -> Value {
	let code = col.type_code();
	if !is_defined_at(col, index) {
		return none_value(code);
	}
	match code {
		ColumnTypeCode::Bool => col
			.data_bytes()
			.get(index / 8)
			.copied()
			.map(|b| Value::Boolean((b >> (index % 8)) & 1 == 1))
			.unwrap_or_else(|| none_value(code)),
		ColumnTypeCode::Float4 => fixed_at::<f32>(col, index)
			.and_then(|v| OrderedF32::try_from(v).ok())
			.map(Value::Float4)
			.unwrap_or_else(|| none_value(code)),
		ColumnTypeCode::Float8 => fixed_at::<f64>(col, index)
			.and_then(|v| OrderedF64::try_from(v).ok())
			.map(Value::Float8)
			.unwrap_or_else(|| none_value(code)),
		ColumnTypeCode::Int1 => fixed_at::<i8>(col, index).map(Value::Int1).unwrap_or_else(|| none_value(code)),
		ColumnTypeCode::Int2 => {
			fixed_at::<i16>(col, index).map(Value::Int2).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Int4 => {
			fixed_at::<i32>(col, index).map(Value::Int4).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Int8 => {
			fixed_at::<i64>(col, index).map(Value::Int8).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Int16 => {
			fixed_at::<i128>(col, index).map(Value::Int16).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Uint1 => {
			fixed_at::<u8>(col, index).map(Value::Uint1).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Uint2 => {
			fixed_at::<u16>(col, index).map(Value::Uint2).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Uint4 => {
			fixed_at::<u32>(col, index).map(Value::Uint4).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Uint8 => {
			fixed_at::<u64>(col, index).map(Value::Uint8).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Uint16 => {
			fixed_at::<u128>(col, index).map(Value::Uint16).unwrap_or_else(|| none_value(code))
		}
		ColumnTypeCode::Utf8 => col
			.iter_str()
			.nth(index)
			.map(|s| Value::Utf8(s.to_string()))
			.unwrap_or_else(|| none_value(code)),
		ColumnTypeCode::Decimal => {
			decode_decimal_at(col, index).map(Value::Decimal).unwrap_or_else(|| none_value(code))
		}
		_ => none_value(code),
	}
}

impl<'a> BorrowedColumns<'a> {
	pub fn row(self, index: usize) -> Option<FFIRowView<'a>> {
		if index >= self.row_count() {
			return None;
		}
		Some(FFIRowView::new(self, index))
	}

	pub fn rows(self) -> impl Iterator<Item = FFIRowView<'a>> {
		(0..self.row_count()).map(move |i| FFIRowView::new(self, i))
	}
}

impl<'a> ColumnsView for BorrowedColumns<'a> {
	fn row_count(&self) -> usize {
		BorrowedColumns::row_count(self)
	}

	fn row(&self, index: usize) -> Option<impl RowView + '_> {
		(*self).row(index)
	}
}

impl<'a> DiffView for BorrowedDiff<'a> {
	fn kind(&self) -> DiffType {
		BorrowedDiff::kind(self)
	}

	fn pre(&self) -> Option<impl ColumnsView + '_> {
		match self.kind() {
			DiffType::Update | DiffType::Remove => Some(self.pre()),
			DiffType::Insert => None,
		}
	}

	fn post(&self) -> Option<impl ColumnsView + '_> {
		match self.kind() {
			DiffType::Insert | DiffType::Update => Some(self.post()),
			DiffType::Remove => None,
		}
	}
}

impl<'a> ChangeView for BorrowedChange<'a> {
	fn version(&self) -> u64 {
		BorrowedChange::version(self)
	}

	fn changed_at_nanos(&self) -> u64 {
		BorrowedChange::changed_at_nanos(self)
	}

	fn diff_count(&self) -> usize {
		BorrowedChange::diff_count(self)
	}

	fn diff(&self, index: usize) -> Option<impl DiffView + '_> {
		self.diffs().nth(index)
	}
}
