// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::any::type_name;

use reifydb_codec::tag::{TypeTag, ValueKind};
use reifydb_value::{
	error::ColumnReadReason,
	value::{
		date::Date, datetime::DateTime, decimal::Decimal, diff_type::DiffType, duration::Duration,
		row_number::RowNumber, system_columns::RowTimes, time::Time, value_type::ValueType,
	},
};

use crate::{
	common::{
		buffer_check::{bitvec_len, fixed_len},
		family::family_params,
	},
	error::{Result as SdkResult, SdkError},
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

	fn column_defined(&self, name: &str) -> SdkResult<Option<BorrowedColumn<'a>>> {
		let Some(col) = self.columns.column(name)? else {
			return Ok(None);
		};
		if !col.defined_at(self.index)? {
			return Ok(None);
		}
		Ok(Some(col))
	}

	fn temporal<T: Copy>(&self, name: &str, kind: ValueKind) -> Result<Option<T>, SdkError> {
		let Some(col) = self.column_defined(name)? else {
			return Ok(None);
		};
		if col.type_code() != kind {
			return Err(read_error::<T>(&col, ColumnReadReason::WrongType));
		}
		fixed_at::<T>(&col, self.index)
	}
}

macro_rules! widening_read {
	($self:ident, $name:ident, $t:ty => [$($kind:ident: $source:ty),*]) => {{
		let Some(col) = $self.column_defined($name)? else {
			return Ok(None);
		};
		match col.type_code() {
			$(ValueKind::$kind => Ok(fixed_at::<$source>(&col, $self.index)?.map(<$t>::from)),)*
			_ => Err(read_error::<$t>(&col, ColumnReadReason::WrongType)),
		}
	}};
}

impl<'a> RowView for ExternCRowView<'a> {
	fn is_defined(&self, name: &str) -> Result<bool, SdkError> {
		match self.columns.column(name)? {
			Some(col) => col.defined_at(self.index),
			None => Ok(false),
		}
	}

	fn utf8(&self, name: &str) -> Result<Option<&str>, SdkError> {
		let Some(col) = self.column_defined(name)? else {
			return Ok(None);
		};
		if col.type_code() != ValueKind::Utf8 {
			return Err(read_error::<&str>(&col, ColumnReadReason::WrongType));
		}
		col.str_at(self.index)
	}

	fn blob(&self, name: &str) -> Result<Option<&[u8]>, SdkError> {
		let Some(col) = self.column_defined(name)? else {
			return Ok(None);
		};
		if col.type_code() != ValueKind::Blob {
			return Err(read_error::<&[u8]>(&col, ColumnReadReason::WrongType));
		}
		col.bytes_at(self.index)
	}

	fn bool(&self, name: &str) -> Result<Option<bool>, SdkError> {
		let Some(col) = self.column_defined(name)? else {
			return Ok(None);
		};
		if col.type_code() != ValueKind::Boolean {
			return Err(read_error::<bool>(&col, ColumnReadReason::WrongType));
		}
		bitvec_len(col.data_bytes().len(), col.row_count(), col.name())?;
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

	fn decimal(&self, name: &str) -> Result<Option<Decimal>, SdkError> {
		let Some(col) = self.column_defined(name)? else {
			return Ok(None);
		};
		let does_not_fit = || read_error::<Decimal>(&col, ColumnReadReason::DoesNotFit);
		match col.type_code() {
			ValueKind::Decimal => match col.family_cell_at::<Decimal>(self.index)? {
				Some(decoded) => decoded.map(Some).map_err(|_| does_not_fit()),
				None => Ok(None),
			},
			ValueKind::Float8 => fixed_at::<f64>(&col, self.index)?
				.map(|v| Decimal::from_f64(v).ok_or_else(does_not_fit))
				.transpose(),
			ValueKind::Float4 => fixed_at::<f32>(&col, self.index)?
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

	fn row_number(&self) -> Option<RowNumber> {
		self.columns.row_numbers().get(self.index).copied()
	}

	fn row_time(&self) -> Option<DateTime> {
		self.columns.times().get(self.index)
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

pub(crate) fn fixed_at<T: Copy>(col: &BorrowedColumn<'_>, index: usize) -> SdkResult<Option<T>> {
	fixed_len(col.data_bytes().len(), col.row_count(), size_of::<T>(), col.name())?;
	// SAFETY: discharges BorrowedColumn::as_slice - every caller checks `col.type_code()` first and
	// instantiates `T` as that code's element type, and the host marshals fixed-width columns
	// zero-copy from a live `&[T]`, so the bytes are an initialised array of `T` aligned for it.
	let slice = unsafe { col.as_slice::<T>() };
	Ok(slice.and_then(|slice| slice.get(index).copied()))
}

fn type_for_column(col: &BorrowedColumn<'_>) -> ValueType {
	let code = col.type_code();
	match code {
		ValueKind::Decimal => match family_params(code, col.precision(), col.scale()) {
			Some((precision, scale)) => ValueType::decimal(precision, scale),
			None => ValueType::Any,
		},
		_ => TypeTag::new(code, 0).ok().and_then(|tag| tag.to_type().ok()).unwrap_or(ValueType::Any),
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

	fn row_numbers(&self) -> &[RowNumber] {
		BorrowedColumns::row_numbers(self)
	}

	fn times(&self) -> RowTimes<'_> {
		BorrowedColumns::times(self)
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

	fn changed_at_nanos(&self) -> i64 {
		BorrowedChange::changed_at_nanos(self)
	}

	fn diff_count(&self) -> usize {
		BorrowedChange::diff_count(self)
	}

	fn diff(&self, index: usize) -> Option<impl DiffView + '_> {
		self.diff_at(index)
	}
}
