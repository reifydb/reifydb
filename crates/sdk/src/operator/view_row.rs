// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use postcard::from_bytes;
use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_type::value::{
	Value, decimal::Decimal, ordered_f32::OrderedF32, ordered_f64::OrderedF64, row_number::RowNumber, r#type::Type,
};
use serde::de::DeserializeOwned;

use crate::operator::change::{BorrowedColumn, BorrowedColumns};

#[derive(Clone, Copy)]
pub struct RowView<'a> {
	columns: BorrowedColumns<'a>,
	index: usize,
}

impl<'a> RowView<'a> {
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

	pub fn row_number(&self) -> Option<RowNumber> {
		self.columns.row_numbers().get(self.index).copied().map(RowNumber)
	}

	pub fn created_at_nanos(&self) -> Option<u64> {
		self.columns.created_at().get(self.index).copied()
	}

	pub fn updated_at_nanos(&self) -> Option<u64> {
		self.columns.updated_at().get(self.index).copied()
	}

	pub fn is_defined(&self, name: &str) -> bool {
		match self.columns.column(name) {
			Some(col) => is_defined_at(&col, self.index),
			None => false,
		}
	}

	pub fn utf8(&self, name: &str) -> Option<&'a str> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Utf8 {
			return None;
		}
		col.iter_str().nth(self.index)
	}

	pub fn blob(&self, name: &str) -> Option<&'a [u8]> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Blob {
			return None;
		}
		col.iter_bytes().nth(self.index)
	}

	pub fn bool(&self, name: &str) -> Option<bool> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Bool {
			return None;
		}
		let bytes = col.data_bytes();
		let byte = bytes.get(self.index / 8).copied()?;
		Some((byte >> (self.index % 8)) & 1 == 1)
	}

	pub fn u64(&self, name: &str) -> Option<u64> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Uint8 => fixed_at::<u64>(&col, self.index),
			ColumnTypeCode::Uint4 => fixed_at::<u32>(&col, self.index).map(u64::from),
			ColumnTypeCode::Uint2 => fixed_at::<u16>(&col, self.index).map(u64::from),
			ColumnTypeCode::Uint1 => fixed_at::<u8>(&col, self.index).map(u64::from),
			_ => None,
		}
	}

	pub fn u32(&self, name: &str) -> Option<u32> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Uint4 => fixed_at::<u32>(&col, self.index),
			ColumnTypeCode::Uint2 => fixed_at::<u16>(&col, self.index).map(u32::from),
			ColumnTypeCode::Uint1 => fixed_at::<u8>(&col, self.index).map(u32::from),
			_ => None,
		}
	}

	pub fn u16(&self, name: &str) -> Option<u16> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Uint2 => fixed_at::<u16>(&col, self.index),
			ColumnTypeCode::Uint1 => fixed_at::<u8>(&col, self.index).map(u16::from),
			_ => None,
		}
	}

	pub fn u8(&self, name: &str) -> Option<u8> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Uint1 {
			return None;
		}
		fixed_at::<u8>(&col, self.index)
	}

	pub fn i64(&self, name: &str) -> Option<i64> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Int8 => fixed_at::<i64>(&col, self.index),
			ColumnTypeCode::Int4 => fixed_at::<i32>(&col, self.index).map(i64::from),
			ColumnTypeCode::Int2 => fixed_at::<i16>(&col, self.index).map(i64::from),
			ColumnTypeCode::Int1 => fixed_at::<i8>(&col, self.index).map(i64::from),
			_ => None,
		}
	}

	pub fn i32(&self, name: &str) -> Option<i32> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Int4 => fixed_at::<i32>(&col, self.index),
			ColumnTypeCode::Int2 => fixed_at::<i16>(&col, self.index).map(i32::from),
			ColumnTypeCode::Int1 => fixed_at::<i8>(&col, self.index).map(i32::from),
			_ => None,
		}
	}

	pub fn i16(&self, name: &str) -> Option<i16> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Int2 => fixed_at::<i16>(&col, self.index),
			ColumnTypeCode::Int1 => fixed_at::<i8>(&col, self.index).map(i16::from),
			_ => None,
		}
	}

	pub fn i8(&self, name: &str) -> Option<i8> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Int1 {
			return None;
		}
		fixed_at::<i8>(&col, self.index)
	}

	pub fn f64(&self, name: &str) -> Option<f64> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Float8 => fixed_at::<f64>(&col, self.index),
			ColumnTypeCode::Float4 => fixed_at::<f32>(&col, self.index).map(f64::from),
			_ => None,
		}
	}

	pub fn f32(&self, name: &str) -> Option<f32> {
		let col = self.column_defined(name)?;
		if col.type_code() != ColumnTypeCode::Float4 {
			return None;
		}
		fixed_at::<f32>(&col, self.index)
	}

	pub fn decimal(&self, name: &str) -> Option<Decimal> {
		let col = self.column_defined(name)?;
		match col.type_code() {
			ColumnTypeCode::Decimal => decode_serialized_at::<Decimal>(&col, self.index),
			ColumnTypeCode::Float8 => fixed_at::<f64>(&col, self.index).map(Decimal::from),
			ColumnTypeCode::Float4 => fixed_at::<f32>(&col, self.index).map(|v| Decimal::from(v as f64)),
			_ => None,
		}
	}

	pub fn value(&self, name: &str) -> Option<Value> {
		let col = self.columns.column(name)?;
		Some(read_value_at(&col, self.index))
	}

	fn column_defined(&self, name: &str) -> Option<BorrowedColumn<'a>> {
		let col = self.columns.column(name)?;
		if !is_defined_at(&col, self.index) {
			return None;
		}
		Some(col)
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

pub(crate) fn decode_serialized_at<T>(col: &BorrowedColumn<'_>, index: usize) -> Option<T>
where
	T: DeserializeOwned,
{
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
	from_bytes::<T>(&data[start..end]).ok()
}

fn type_for_code(code: ColumnTypeCode) -> Type {
	match code {
		ColumnTypeCode::Bool => Type::Boolean,
		ColumnTypeCode::Float4 => Type::Float4,
		ColumnTypeCode::Float8 => Type::Float8,
		ColumnTypeCode::Int1 => Type::Int1,
		ColumnTypeCode::Int2 => Type::Int2,
		ColumnTypeCode::Int4 => Type::Int4,
		ColumnTypeCode::Int8 => Type::Int8,
		ColumnTypeCode::Int16 => Type::Int16,
		ColumnTypeCode::Uint1 => Type::Uint1,
		ColumnTypeCode::Uint2 => Type::Uint2,
		ColumnTypeCode::Uint4 => Type::Uint4,
		ColumnTypeCode::Uint8 => Type::Uint8,
		ColumnTypeCode::Uint16 => Type::Uint16,
		ColumnTypeCode::Utf8 => Type::Utf8,
		ColumnTypeCode::Decimal => Type::Decimal,
		ColumnTypeCode::Blob => Type::Blob,
		_ => Type::Any,
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
		ColumnTypeCode::Decimal => decode_serialized_at::<Decimal>(col, index)
			.map(Value::Decimal)
			.unwrap_or_else(|| none_value(code)),
		_ => none_value(code),
	}
}

impl<'a> BorrowedColumns<'a> {
	pub fn row(self, index: usize) -> Option<RowView<'a>> {
		if index >= self.row_count() {
			return None;
		}
		Some(RowView::new(self, index))
	}

	pub fn rows(self) -> impl Iterator<Item = RowView<'a>> {
		(0..self.row_count()).map(move |i| RowView::new(self, i))
	}
}
