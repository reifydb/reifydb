// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::any::type_name;

use reifydb_codec::tag::ValueKind;
use reifydb_value::{
	error::ColumnReadReason,
	value::{
		Value, date::Date, datetime::DateTime, decimal::Decimal, diff_type::DiffType, duration::Duration,
		int::Int, ordered_f32::OrderedF32, ordered_f64::OrderedF64, row_number::RowNumber, time::Time,
		uint::Uint, value_type::ValueType,
	},
};

use crate::{
	common::family::{FamilyValue, family_params},
	error::SdkError,
	flow::operator::{
		change::{BorrowedChange, BorrowedColumn, BorrowedColumns, BorrowedDiff},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
};

#[derive(Clone, Copy)]
pub struct ExternCRowView<'a> {
	columns: BorrowedColumns<'a>,
	index: usize,
}

impl<'a> ExternCRowView<'a> {
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

	fn family<T: FamilyValue>(&self, name: &str) -> Result<Option<T>, SdkError> {
		let Some(col) = self.column_defined(name) else {
			return Ok(None);
		};
		if col.type_code() != T::KIND {
			return Err(read_error::<T>(&col, ColumnReadReason::WrongType));
		}
		col.family_cell_at(self.index)
	}

	fn temporal<T: Copy>(&self, name: &str, kind: ValueKind) -> Result<Option<T>, SdkError> {
		let Some(col) = self.column_defined(name) else {
			return Ok(None);
		};
		if col.type_code() != kind {
			return Err(read_error::<T>(&col, ColumnReadReason::WrongType));
		}
		Ok(fixed_at::<T>(&col, self.index))
	}
}

macro_rules! widening_read {
	($self:ident, $name:ident, $t:ty => [$($kind:ident: $source:ty),*]) => {{
		let Some(col) = $self.column_defined($name) else {
			return Ok(None);
		};
		match col.type_code() {
			$(ValueKind::$kind => Ok(fixed_at::<$source>(&col, $self.index).map(<$t>::from)),)*
			_ => Err(read_error::<$t>(&col, ColumnReadReason::WrongType)),
		}
	}};
}

impl<'a> RowView for ExternCRowView<'a> {
	fn is_defined(&self, name: &str) -> bool {
		match self.columns.column(name) {
			Some(col) => is_defined_at(&col, self.index),
			None => false,
		}
	}

	fn utf8(&self, name: &str) -> Result<Option<&str>, SdkError> {
		let Some(col) = self.column_defined(name) else {
			return Ok(None);
		};
		if col.type_code() != ValueKind::Utf8 {
			return Err(read_error::<&str>(&col, ColumnReadReason::WrongType));
		}
		Ok(col.iter_str().nth(self.index))
	}

	fn blob(&self, name: &str) -> Result<Option<&[u8]>, SdkError> {
		let Some(col) = self.column_defined(name) else {
			return Ok(None);
		};
		if col.type_code() != ValueKind::Blob {
			return Err(read_error::<&[u8]>(&col, ColumnReadReason::WrongType));
		}
		Ok(col.iter_bytes().nth(self.index))
	}

	fn bool(&self, name: &str) -> Result<Option<bool>, SdkError> {
		let Some(col) = self.column_defined(name) else {
			return Ok(None);
		};
		if col.type_code() != ValueKind::Boolean {
			return Err(read_error::<bool>(&col, ColumnReadReason::WrongType));
		}
		Ok(col.data_bytes().get(self.index / 8).map(|byte| (byte >> (self.index % 8)) & 1 == 1))
	}

	fn u8(&self, name: &str) -> Result<Option<u8>, SdkError> {
		widening_read!(self, name, u8 => [Uint1: u8])
	}

	fn u16(&self, name: &str) -> Result<Option<u16>, SdkError> {
		widening_read!(self, name, u16 => [Uint1: u8, Uint2: u16])
	}

	fn u32(&self, name: &str) -> Result<Option<u32>, SdkError> {
		widening_read!(self, name, u32 => [Uint1: u8, Uint2: u16, Uint4: u32])
	}

	fn u64(&self, name: &str) -> Result<Option<u64>, SdkError> {
		widening_read!(self, name, u64 => [Uint1: u8, Uint2: u16, Uint4: u32, Uint8: u64])
	}

	fn u128(&self, name: &str) -> Result<Option<u128>, SdkError> {
		widening_read!(self, name, u128 => [Uint1: u8, Uint2: u16, Uint4: u32, Uint8: u64, Uint16: u128])
	}

	fn i8(&self, name: &str) -> Result<Option<i8>, SdkError> {
		widening_read!(self, name, i8 => [Int1: i8])
	}

	fn i16(&self, name: &str) -> Result<Option<i16>, SdkError> {
		widening_read!(self, name, i16 => [Int1: i8, Int2: i16])
	}

	fn i32(&self, name: &str) -> Result<Option<i32>, SdkError> {
		widening_read!(self, name, i32 => [Int1: i8, Int2: i16, Int4: i32])
	}

	fn i64(&self, name: &str) -> Result<Option<i64>, SdkError> {
		widening_read!(self, name, i64 => [Int1: i8, Int2: i16, Int4: i32, Int8: i64])
	}

	fn i128(&self, name: &str) -> Result<Option<i128>, SdkError> {
		widening_read!(self, name, i128 => [Int1: i8, Int2: i16, Int4: i32, Int8: i64, Int16: i128])
	}

	fn f32(&self, name: &str) -> Result<Option<f32>, SdkError> {
		widening_read!(self, name, f32 => [Float4: f32])
	}

	fn f64(&self, name: &str) -> Result<Option<f64>, SdkError> {
		widening_read!(self, name, f64 => [Float4: f32, Float8: f64])
	}

	fn int(&self, name: &str) -> Result<Option<Int>, SdkError> {
		self.family(name)
	}

	fn uint(&self, name: &str) -> Result<Option<Uint>, SdkError> {
		self.family(name)
	}

	fn decimal(&self, name: &str) -> Result<Option<Decimal>, SdkError> {
		let Some(col) = self.column_defined(name) else {
			return Ok(None);
		};
		let does_not_fit = || read_error::<Decimal>(&col, ColumnReadReason::DoesNotFit);
		match col.type_code() {
			ValueKind::Decimal => col.family_cell_at(self.index),
			ValueKind::Float8 => fixed_at::<f64>(&col, self.index)
				.map(|v| Decimal::from_f64(v).ok_or_else(does_not_fit))
				.transpose(),
			ValueKind::Float4 => fixed_at::<f32>(&col, self.index)
				.map(|v| Decimal::from_f32(v).ok_or_else(does_not_fit))
				.transpose(),
			_ => Err(read_error::<Decimal>(&col, ColumnReadReason::WrongType)),
		}
	}

	fn date(&self, name: &str) -> Result<Option<Date>, SdkError> {
		self.temporal(name, ValueKind::Date)
	}

	fn datetime(&self, name: &str) -> Result<Option<DateTime>, SdkError> {
		self.temporal(name, ValueKind::DateTime)
	}

	fn time(&self, name: &str) -> Result<Option<Time>, SdkError> {
		self.temporal(name, ValueKind::Time)
	}

	fn duration(&self, name: &str) -> Result<Option<Duration>, SdkError> {
		self.temporal(name, ValueKind::Duration)
	}

	fn value(&self, name: &str) -> Option<Value> {
		let col = self.columns.column(name)?;
		Some(read_value_at(&col, self.index))
	}

	fn row_number(&self) -> Option<RowNumber> {
		self.columns.row_numbers().get(self.index).copied().map(RowNumber)
	}

	fn row_time(&self) -> Option<DateTime> {
		self.columns.time().get(self.index).copied().map(DateTime::from_nanos)
	}
}

fn read_error<T: ?Sized>(col: &BorrowedColumn<'_>, reason: ColumnReadReason) -> SdkError {
	SdkError::ColumnRead {
		column: col.name().to_string(),
		column_type: type_for_column(col),
		target: type_name::<T>(),
		reason,
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
	// SAFETY: discharges BorrowedColumn::as_slice - every caller checks `col.type_code()` first and
	// instantiates `T` as that code's element type, and the host marshals fixed-width columns
	// zero-copy from a live `&[T]`, so the bytes are an initialised array of `T` aligned for it.
	let slice = unsafe { col.as_slice::<T>()? };
	slice.get(index).copied()
}

fn type_for_column(col: &BorrowedColumn<'_>) -> ValueType {
	let code = col.type_code();
	match code {
		ValueKind::Boolean => ValueType::Boolean,
		ValueKind::Float4 => ValueType::Float4,
		ValueKind::Float8 => ValueType::Float8,
		ValueKind::Int1 => ValueType::Int1,
		ValueKind::Int2 => ValueType::Int2,
		ValueKind::Int4 => ValueType::Int4,
		ValueKind::Int8 => ValueType::Int8,
		ValueKind::Int16 => ValueType::Int16,
		ValueKind::Uint1 => ValueType::Uint1,
		ValueKind::Uint2 => ValueType::Uint2,
		ValueKind::Uint4 => ValueType::Uint4,
		ValueKind::Uint8 => ValueType::Uint8,
		ValueKind::Uint16 => ValueType::Uint16,
		ValueKind::Utf8 => ValueType::Utf8,
		ValueKind::Int | ValueKind::Uint | ValueKind::Decimal => {
			match family_params(code, col.precision(), col.scale()) {
				Some((precision, _)) if code == ValueKind::Int => ValueType::int(precision),
				Some((precision, _)) if code == ValueKind::Uint => ValueType::uint(precision),
				Some((precision, scale)) => ValueType::decimal(precision, scale),
				None => ValueType::Any,
			}
		}
		ValueKind::Blob => ValueType::Blob,
		_ => ValueType::Any,
	}
}

fn none_value(col: &BorrowedColumn<'_>) -> Value {
	Value::None {
		inner: type_for_column(col),
	}
}

fn read_value_at(col: &BorrowedColumn<'_>, index: usize) -> Value {
	let code = col.type_code();
	if !is_defined_at(col, index) {
		return none_value(col);
	}
	match code {
		ValueKind::Boolean => col
			.data_bytes()
			.get(index / 8)
			.copied()
			.map(|b| Value::Boolean((b >> (index % 8)) & 1 == 1))
			.unwrap_or_else(|| none_value(col)),
		ValueKind::Float4 => fixed_at::<f32>(col, index)
			.and_then(|v| OrderedF32::try_from(v).ok())
			.map(Value::Float4)
			.unwrap_or_else(|| none_value(col)),
		ValueKind::Float8 => fixed_at::<f64>(col, index)
			.and_then(|v| OrderedF64::try_from(v).ok())
			.map(Value::Float8)
			.unwrap_or_else(|| none_value(col)),
		ValueKind::Int1 => fixed_at::<i8>(col, index).map(Value::Int1).unwrap_or_else(|| none_value(col)),
		ValueKind::Int2 => fixed_at::<i16>(col, index).map(Value::Int2).unwrap_or_else(|| none_value(col)),
		ValueKind::Int4 => fixed_at::<i32>(col, index).map(Value::Int4).unwrap_or_else(|| none_value(col)),
		ValueKind::Int8 => fixed_at::<i64>(col, index).map(Value::Int8).unwrap_or_else(|| none_value(col)),
		ValueKind::Int16 => fixed_at::<i128>(col, index).map(Value::Int16).unwrap_or_else(|| none_value(col)),
		ValueKind::Uint1 => fixed_at::<u8>(col, index).map(Value::Uint1).unwrap_or_else(|| none_value(col)),
		ValueKind::Uint2 => fixed_at::<u16>(col, index).map(Value::Uint2).unwrap_or_else(|| none_value(col)),
		ValueKind::Uint4 => fixed_at::<u32>(col, index).map(Value::Uint4).unwrap_or_else(|| none_value(col)),
		ValueKind::Uint8 => fixed_at::<u64>(col, index).map(Value::Uint8).unwrap_or_else(|| none_value(col)),
		ValueKind::Uint16 => fixed_at::<u128>(col, index).map(Value::Uint16).unwrap_or_else(|| none_value(col)),
		ValueKind::Utf8 => {
			col.iter_str().nth(index).map(|s| Value::Utf8(s.to_string())).unwrap_or_else(|| none_value(col))
		}
		ValueKind::Int => col.expect_family_cell_at(index).map(Value::Int).unwrap_or_else(|| none_value(col)),
		ValueKind::Uint => col.expect_family_cell_at(index).map(Value::Uint).unwrap_or_else(|| none_value(col)),
		ValueKind::Decimal => {
			col.expect_family_cell_at(index).map(Value::Decimal).unwrap_or_else(|| none_value(col))
		}
		_ => none_value(col),
	}
}

impl<'a> BorrowedColumns<'a> {
	pub fn row(self, index: usize) -> Option<ExternCRowView<'a>> {
		if index >= self.row_count() {
			return None;
		}
		Some(ExternCRowView::new(self, index))
	}

	pub fn rows(self) -> impl Iterator<Item = ExternCRowView<'a>> {
		(0..self.row_count()).map(move |i| ExternCRowView::new(self, i))
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

fn diff_kind(code: DiffType) -> DiffType {
	code
}

impl<'a> DiffView for BorrowedDiff<'a> {
	fn kind(&self) -> DiffType {
		diff_kind(BorrowedDiff::kind(self))
	}

	fn pre(&self) -> Option<impl ColumnsView + '_> {
		match BorrowedDiff::kind(self) {
			DiffType::Update | DiffType::Remove => Some(self.pre()),
			DiffType::Insert => None,
		}
	}

	fn post(&self) -> Option<impl ColumnsView + '_> {
		match BorrowedDiff::kind(self) {
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
