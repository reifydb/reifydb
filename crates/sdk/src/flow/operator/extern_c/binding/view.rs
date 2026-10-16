// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::type_name, cell::OnceCell, collections::HashMap, marker::PhantomData, rc::Rc};

use reifydb_codec::tag::{TypeTag, ValueKind};
use reifydb_value::{
	error::ColumnReadReason,
	value::{
		Value,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		diff_type::DiffType,
		duration::Duration,
		row_number::RowNumber,
		system_columns::RowTimes,
		time::Time,
		try_from::{FromValueError, TryFromValue},
		value_type::ValueType,
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
		view::{
			BorrowedCell, ChangeView, ColumnCursor, ColumnsView, CursorValue, DiffView, RowView,
			in_process::DictionaryValues,
		},
	},
};

type Entries<'a> = Option<&'a HashMap<DictionaryEntryId, Value>>;

struct ExternCNames<'a> {
	columns: BorrowedColumns<'a>,
	dictionary: Option<&'a DictionaryValues>,
	positions: OnceCell<Option<HashMap<&'a str, (usize, BorrowedColumn<'a>)>>>,
}

impl<'a> ExternCNames<'a> {
	fn new(columns: BorrowedColumns<'a>, dictionary: Option<&'a DictionaryValues>) -> Rc<Self> {
		Rc::new(Self {
			columns,
			dictionary,
			positions: OnceCell::new(),
		})
	}

	fn column(&self, name: &str) -> SdkResult<Option<(BorrowedColumn<'a>, Entries<'a>)>> {
		let positions = self.positions.get_or_init(|| {
			let mut positions = HashMap::with_capacity(self.columns.column_count());
			for (index, column) in self.columns.columns().enumerate() {
				positions.entry(column.checked_name().ok()?).or_insert((index, column));
			}
			Some(positions)
		});
		let found = match positions {
			Some(positions) => positions.get(name).copied(),
			None => {
				let mut found = None;
				for (index, column) in self.columns.columns().enumerate() {
					if column.checked_name()? == name {
						found = Some((index, column));
						break;
					}
				}
				found
			}
		};
		found.map(|(index, column)| {
			Ok((
				self.columns.checked_rows(name, column)?,
				self.dictionary.and_then(|dictionary| dictionary.get(&index)),
			))
		})
		.transpose()
	}
}

fn read_cell<'a, T: BorrowedCell<'a>>(
	col: &BorrowedColumn<'a>,
	entries: Entries<'a>,
	index: usize,
) -> SdkResult<Option<T>> {
	if !col.defined_at(index)? {
		return Ok(None);
	}
	let Some(entries) = entries else {
		return T::read_borrowed(col, index);
	};
	match col.dictionary_id_at(index)?.and_then(|id| entries.get(&id)) {
		Some(value) => T::read_value(value, col),
		None => Ok(None),
	}
}

fn value_cell<T: TryFromValue>(value: &Value, col: &BorrowedColumn<'_>) -> SdkResult<Option<T>> {
	match value {
		Value::None {
			..
		} => Ok(None),
		value => T::try_from_value(value).map(Some).map_err(|err| {
			let reason = match err {
				FromValueError::TypeMismatch {
					..
				} => ColumnReadReason::WrongType,
				FromValueError::OutOfRange {
					..
				} => ColumnReadReason::DoesNotFit,
			};
			read_error::<T>(col, reason)
		}),
	}
}

#[derive(Clone)]
pub struct ExternCRowView<'a> {
	names: Rc<ExternCNames<'a>>,
	index: usize,
}

impl<'a> ExternCRowView<'a> {
	fn read<T: BorrowedCell<'a>>(&self, name: &str) -> SdkResult<Option<T>> {
		match self.names.column(name)? {
			Some((col, entries)) => read_cell(&col, entries, self.index),
			None => Ok(None),
		}
	}
}

impl<'a> RowView for ExternCRowView<'a> {
	fn is_defined(&self, name: &str) -> Result<bool, SdkError> {
		let Some((col, entries)) = self.names.column(name)? else {
			return Ok(false);
		};
		if !col.defined_at(self.index)? {
			return Ok(false);
		}
		match entries {
			Some(entries) => {
				Ok(col.dictionary_id_at(self.index)?.is_some_and(|id| entries.contains_key(&id)))
			}
			None => Ok(true),
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
		self.names.columns.row_numbers().get(self.index).copied()
	}

	fn row_time(&self) -> Option<DateTime> {
		self.names.columns.times().get(self.index)
	}
}

impl<'a> BorrowedCell<'a> for &'a str {
	fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
		if col.type_code() != ValueKind::Utf8 {
			return Err(read_error::<&str>(col, ColumnReadReason::WrongType));
		}
		col.str_at(index)
	}

	fn read_value(value: &'a Value, col: &BorrowedColumn<'_>) -> SdkResult<Option<Self>> {
		match value {
			Value::Utf8(text) => Ok(Some(text.as_str())),
			Value::None {
				..
			} => Ok(None),
			_ => Err(read_error::<&str>(col, ColumnReadReason::WrongType)),
		}
	}
}

impl<'a> BorrowedCell<'a> for &'a [u8] {
	fn read_borrowed(col: &BorrowedColumn<'a>, index: usize) -> SdkResult<Option<Self>> {
		if col.type_code() != ValueKind::Blob {
			return Err(read_error::<&[u8]>(col, ColumnReadReason::WrongType));
		}
		col.bytes_at(index)
	}

	fn read_value(value: &'a Value, col: &BorrowedColumn<'_>) -> SdkResult<Option<Self>> {
		match value {
			Value::Blob(blob) => Ok(Some(blob.as_bytes())),
			Value::None {
				..
			} => Ok(None),
			_ => Err(read_error::<&[u8]>(col, ColumnReadReason::WrongType)),
		}
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

	fn read_value(value: &'a Value, col: &BorrowedColumn<'_>) -> SdkResult<Option<Self>> {
		value_cell(value, col)
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

			fn read_value(value: &'a Value, col: &BorrowedColumn<'_>) -> SdkResult<Option<Self>> {
				value_cell(value, col)
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

	fn read_value(value: &'a Value, col: &BorrowedColumn<'_>) -> SdkResult<Option<Self>> {
		value_cell(value, col)
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

			fn read_value(value: &'a Value, col: &BorrowedColumn<'_>) -> SdkResult<Option<Self>> {
				value_cell(value, col)
			}
		}
	)*};
}

temporal_cell!(Date => Date, DateTime => DateTime, Time => Time, Duration => Duration);

pub struct ExternCCursor<'a, T> {
	col: BorrowedColumn<'a>,
	entries: Entries<'a>,
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
		read_cell(&self.col, self.entries, index)
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
		Some(ExternCRowView {
			names: ExternCNames::new(self, None),
			index,
		})
	}

	pub fn rows(self) -> impl Iterator<Item = ExternCRowView<'a>> {
		let names = ExternCNames::new(self, None);
		(0..self.row_count()).map(move |index| ExternCRowView {
			names: Rc::clone(&names),
			index,
		})
	}
}

struct ExternCColumnsView<'a> {
	names: Rc<ExternCNames<'a>>,
}

impl<'a> ColumnsView for ExternCColumnsView<'a> {
	fn row_count(&self) -> usize {
		self.names.columns.row_count()
	}

	fn column<'c, T: CursorValue<'c> + 'c>(&'c self, name: &str) -> SdkResult<Option<impl ColumnCursor<T> + 'c>> {
		Ok(self.names.column(name)?.map(|(col, entries)| ExternCCursor {
			col,
			entries,
			_value: PhantomData,
		}))
	}

	fn row(&self, index: usize) -> Option<impl RowView + '_> {
		if index >= self.names.columns.row_count() {
			return None;
		}
		Some(ExternCRowView {
			names: Rc::clone(&self.names),
			index,
		})
	}

	fn row_numbers(&self) -> &[RowNumber] {
		self.names.columns.row_numbers()
	}

	fn times(&self) -> RowTimes<'_> {
		self.names.columns.times()
	}
}

impl<'a> ColumnsView for BorrowedColumns<'a> {
	fn row_count(&self) -> usize {
		BorrowedColumns::row_count(self)
	}

	fn column<'c, T: CursorValue<'c> + 'c>(&'c self, name: &str) -> SdkResult<Option<impl ColumnCursor<T> + 'c>> {
		Ok(BorrowedColumns::column(self, name)?.map(|col| ExternCCursor {
			col,
			entries: None,
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

pub struct ExternCChangeView<'a> {
	change: BorrowedChange<'a>,
	dictionary: &'a [[DictionaryValues; 2]],
}

impl<'a> ExternCChangeView<'a> {
	pub fn new(change: BorrowedChange<'a>, dictionary: &'a [[DictionaryValues; 2]]) -> Self {
		Self {
			change,
			dictionary,
		}
	}
}

struct ExternCDiffView<'a> {
	diff: BorrowedDiff<'a>,
	dictionary: Option<&'a [DictionaryValues; 2]>,
}

impl<'a> DiffView for ExternCDiffView<'a> {
	fn kind(&self) -> DiffType {
		self.diff.kind()
	}

	fn pre(&self) -> Option<impl ColumnsView + '_> {
		match self.diff.kind() {
			DiffType::Update | DiffType::Remove => Some(ExternCColumnsView {
				names: ExternCNames::new(self.diff.pre(), self.dictionary.map(|[pre, _]| pre)),
			}),
			DiffType::Insert => None,
		}
	}

	fn post(&self) -> Option<impl ColumnsView + '_> {
		match self.diff.kind() {
			DiffType::Insert | DiffType::Update => Some(ExternCColumnsView {
				names: ExternCNames::new(self.diff.post(), self.dictionary.map(|[_, post]| post)),
			}),
			DiffType::Remove => None,
		}
	}
}

impl<'a> ChangeView for ExternCChangeView<'a> {
	fn version(&self) -> u64 {
		self.change.version()
	}

	fn changed_at_nanos(&self) -> i64 {
		self.change.changed_at_nanos()
	}

	fn diff_count(&self) -> usize {
		self.change.diff_count()
	}

	fn diff(&self, index: usize) -> Option<impl DiffView + '_> {
		self.change.diff_at(index).map(|diff| ExternCDiffView {
			diff,
			dictionary: self.dictionary.get(index),
		})
	}
}
