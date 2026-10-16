// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::type_name, marker::PhantomData};

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
		view::{BorrowedCell, ChangeView, ColumnCursor, ColumnsView, CursorValue, DiffView, RowView},
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

	fn read<T: BorrowedCell<'a>>(&self, name: &str) -> SdkResult<Option<T>> {
		let Some(col) = self.columns.column(name)? else {
			return Ok(None);
		};
		if !col.defined_at(self.index)? {
			return Ok(None);
		}
		T::read_borrowed(&col, self.index)
	}
}

impl<'a> RowView for ExternCRowView<'a> {
	fn is_defined(&self, name: &str) -> Result<bool, SdkError> {
		match self.columns.column(name)? {
			Some(col) => col.defined_at(self.index),
			None => Ok(false),
		}
	}

	fn utf8(&self, name: &str) -> Result<Option<&str>, SdkError> {
		self.read(name)
	}

	fn blob(&self, name: &str) -> Result<Option<&[u8]>, SdkError> {
		self.read(name)
	}

	fn bool(&self, name: &str) -> Result<Option<bool>, SdkError> {
		self.read(name)
	}

	fn u8(&self, name: &str) -> Result<Option<u8>, SdkError> {
		self.read(name)
	}

	fn u16(&self, name: &str) -> Result<Option<u16>, SdkError> {
		self.read(name)
	}

	fn u32(&self, name: &str) -> Result<Option<u32>, SdkError> {
		self.read(name)
	}

	fn u64(&self, name: &str) -> Result<Option<u64>, SdkError> {
		self.read(name)
	}

	fn u128(&self, name: &str) -> Result<Option<u128>, SdkError> {
		self.read(name)
	}

	fn i8(&self, name: &str) -> Result<Option<i8>, SdkError> {
		self.read(name)
	}

	fn i16(&self, name: &str) -> Result<Option<i16>, SdkError> {
		self.read(name)
	}

	fn i32(&self, name: &str) -> Result<Option<i32>, SdkError> {
		self.read(name)
	}

	fn i64(&self, name: &str) -> Result<Option<i64>, SdkError> {
		self.read(name)
	}

	fn i128(&self, name: &str) -> Result<Option<i128>, SdkError> {
		self.read(name)
	}

	fn f32(&self, name: &str) -> Result<Option<f32>, SdkError> {
		self.read(name)
	}

	fn f64(&self, name: &str) -> Result<Option<f64>, SdkError> {
		self.read(name)
	}

	fn decimal(&self, name: &str) -> Result<Option<Decimal>, SdkError> {
		self.read(name)
	}

	fn date(&self, name: &str) -> Result<Option<Date>, SdkError> {
		self.read(name)
	}

	fn datetime(&self, name: &str) -> Result<Option<DateTime>, SdkError> {
		self.read(name)
	}

	fn time(&self, name: &str) -> Result<Option<Time>, SdkError> {
		self.read(name)
	}

	fn duration(&self, name: &str) -> Result<Option<Duration>, SdkError> {
		self.read(name)
	}

	fn row_number(&self) -> Option<RowNumber> {
		self.columns.row_numbers().get(self.index).copied()
	}

	fn row_time(&self) -> Option<DateTime> {
		self.columns.times().get(self.index)
	}
}

impl<'a> BorrowedCell<'a> for &'a str {
	fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
		if col.type_code() != ValueKind::Utf8 {
			return Err(read_error::<&str>(col, ColumnReadReason::WrongType));
		}
		col.str_at(index)
	}
}

impl<'a> BorrowedCell<'a> for &'a [u8] {
	fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
		if col.type_code() != ValueKind::Blob {
			return Err(read_error::<&[u8]>(col, ColumnReadReason::WrongType));
		}
		col.bytes_at(index)
	}
}

impl<'a> BorrowedCell<'a> for bool {
	fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
		if col.type_code() != ValueKind::Boolean {
			return Err(read_error::<bool>(col, ColumnReadReason::WrongType));
		}
		bitvec_len(col.data_bytes().len(), col.row_count(), col.name())?;
		Ok(col.data_bytes().get(index / 8).map(|byte| (byte >> (index % 8)) & 1 == 1))
	}
}

macro_rules! widening_cell {
	($t:ty => [$($kind:ident: $source:ty),*]) => {
		impl<'a> BorrowedCell<'a> for $t {
			fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
				match col.type_code() {
					$(ValueKind::$kind => Ok(fixed_at::<$source>(col, index)?.map(<$t>::from)),)*
					_ => Err(read_error::<$t>(col, ColumnReadReason::WrongType)),
				}
			}
		}
	};
}

widening_cell!(u8 => [Uint1: u8]);
widening_cell!(u16 => [Uint1: u8, Uint2: u16]);
widening_cell!(u32 => [Uint1: u8, Uint2: u16, Uint4: u32]);
widening_cell!(u64 => [Uint1: u8, Uint2: u16, Uint4: u32, Uint8: u64]);
widening_cell!(u128 => [Uint1: u8, Uint2: u16, Uint4: u32, Uint8: u64, Uint16: u128]);
widening_cell!(i8 => [Int1: i8]);
widening_cell!(i16 => [Int1: i8, Int2: i16]);
widening_cell!(i32 => [Int1: i8, Int2: i16, Int4: i32]);
widening_cell!(i64 => [Int1: i8, Int2: i16, Int4: i32, Int8: i64]);
widening_cell!(i128 => [Int1: i8, Int2: i16, Int4: i32, Int8: i64, Int16: i128]);
widening_cell!(f32 => [Float4: f32]);
widening_cell!(f64 => [Float4: f32, Float8: f64]);

impl<'a> BorrowedCell<'a> for Decimal {
	fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
		let does_not_fit = || read_error::<Decimal>(col, ColumnReadReason::DoesNotFit);
		match col.type_code() {
			ValueKind::Decimal => match col.family_cell_at::<Decimal>(index)? {
				Some(decoded) => decoded.map(Some).map_err(|_| does_not_fit()),
				None => Ok(None),
			},
			ValueKind::Float8 => fixed_at::<f64>(col, index)?
				.map(|v| Decimal::from_f64(v).ok_or_else(does_not_fit))
				.transpose(),
			ValueKind::Float4 => fixed_at::<f32>(col, index)?
				.map(|v| Decimal::from_f32(v).ok_or_else(does_not_fit))
				.transpose(),
			_ => Err(read_error::<Decimal>(col, ColumnReadReason::WrongType)),
		}
	}
}

macro_rules! temporal_cell {
	($($t:ty => $kind:ident),*) => {$(
		impl<'a> BorrowedCell<'a> for $t {
			fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
				if col.type_code() != ValueKind::$kind {
					return Err(read_error::<$t>(col, ColumnReadReason::WrongType));
				}
				fixed_at::<$t>(col, index)
			}
		}
	)*};
}

temporal_cell!(Date => Date, DateTime => DateTime, Time => Time, Duration => Duration);

pub struct ExternCCursor<'a, T> {
	col: BorrowedColumn<'a>,
	_value: PhantomData<fn() -> T>,
}

impl<'a, T: BorrowedCell<'a>> ColumnCursor<T> for ExternCCursor<'a, T> {
	fn get(&self, index: usize) -> SdkResult<Option<T>> {
		if index >= self.col.row_count() {
			return Err(SdkError::InvalidInput(format!(
				"row {index} is out of range for column {} with {} rows",
				self.col.name(),
				self.col.row_count()
			)));
		}
		if !self.col.defined_at(index)? {
			return Ok(None);
		}
		T::read_borrowed(&self.col, index)
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

	fn column<'c, T: CursorValue<'c> + 'c>(&'c self, name: &str) -> SdkResult<Option<impl ColumnCursor<T> + 'c>> {
		Ok(BorrowedColumns::column(self, name)?.map(|col| ExternCCursor {
			col,
			_value: PhantomData,
		}))
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
