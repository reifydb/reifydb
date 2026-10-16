// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::type_name,
	collections::HashMap,
	marker::PhantomData,
	sync::{Arc, OnceLock},
};

use arrow_array::RecordBatch;
use reifydb_core::interface::change::{Change, Diff};
use reifydb_value::{
	error::ColumnReadReason,
	value::{
		column_view::{ColumnView, FromColumnView, ViewData},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		diff_type::DiffType,
		duration::Duration,
		row_number::RowNumber,
		system_columns::{RowTimes, is_system_field, row_numbers, row_times},
		time::Time,
	},
};

use super::{ChangeView, ColumnCursor, ColumnsView, CursorValue, DiffView, RowView, ViewCell};
use crate::error::SdkError;

struct ResolvedColumns<'a> {
	batch: &'a RecordBatch,
	positions: HashMap<&'a str, usize>,
	views: Box<[OnceLock<Option<ColumnView<'a>>>]>,
	times: OnceLock<RowTimes<'a>>,
}

impl<'a> ResolvedColumns<'a> {
	fn new(batch: &'a RecordBatch) -> Self {
		let fields = batch.schema_ref().fields();
		let mut positions = HashMap::with_capacity(fields.len());
		for (index, field) in fields.iter().enumerate() {
			positions.entry(field.name().as_str()).or_insert(index);
		}
		Self {
			batch,
			positions,
			views: fields.iter().map(|_| OnceLock::new()).collect(),
			times: OnceLock::new(),
		}
	}

	fn view(&self, name: &str) -> Result<Option<&ColumnView<'a>>, SdkError> {
		let Some(&index) = self.positions.get(name) else {
			return Ok(None);
		};
		let slot = &self.views[index];
		if let Some(view) = slot.get() {
			return Ok(view.as_ref());
		}
		let batch = self.batch;
		let view = ColumnView::try_from((batch.column(index), batch.schema_ref().field(index)))
			.map_err(|err| SdkError::InvalidInput(err.0.message))?;
		Ok(slot.get_or_init(|| Some(view).filter(|view| !is_system_field(view.field))).as_ref())
	}

	fn row_numbers(&self) -> &'a [RowNumber] {
		row_numbers(self.batch).expect("checked at the apply boundary")
	}

	fn times(&self) -> RowTimes<'a> {
		*self.times.get_or_init(|| row_times(self.batch).expect("checked at the apply boundary"))
	}
}

pub struct InProcessRowView<'a> {
	columns: Arc<ResolvedColumns<'a>>,
	index: usize,
}

impl<'a> InProcessRowView<'a> {
	fn buffer(&self, name: &str) -> Result<Option<&ColumnView<'a>>, SdkError> {
		self.columns.view(name)
	}

	fn read<'r, T: ViewCell<'r>>(&'r self, name: &str) -> Result<Option<T>, SdkError> {
		match self.buffer(name)? {
			Some(view) if view.is_defined(self.index) => T::read_view(view, self.index),
			_ => Ok(None),
		}
	}
}

impl<'a> RowView for InProcessRowView<'a> {
	fn is_defined(&self, name: &str) -> Result<bool, SdkError> {
		Ok(self.buffer(name)?.is_some_and(|view| view.is_defined(self.index)))
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

impl<'a> ViewCell<'a> for &'a str {
	fn read_view(view: &ColumnView<'a>, index: usize) -> Result<Option<Self>, SdkError> {
		if !matches!(view.data, ViewData::Utf8 { .. }) {
			return Err(column_read::<&str>(view.field.name(), view, ColumnReadReason::WrongType));
		}
		Ok(view.get_str(index))
	}
}

impl<'a> ViewCell<'a> for &'a [u8] {
	fn read_view(view: &ColumnView<'a>, index: usize) -> Result<Option<Self>, SdkError> {
		if !matches!(view.data, ViewData::Blob { .. }) {
			return Err(column_read::<&[u8]>(view.field.name(), view, ColumnReadReason::WrongType));
		}
		Ok(view.get_bytes(index))
	}
}

macro_rules! view_cell_from_column_view {
	($($t:ty),*) => {$(
		impl<'a> ViewCell<'a> for $t {
			fn read_view(view: &ColumnView<'a>, index: usize) -> Result<Option<Self>, SdkError> {
				<$t as FromColumnView>::from_column_view(view, index)
					.map_err(|reason| column_read::<$t>(view.field.name(), view, reason))
			}
		}
	)*};
}

view_cell_from_column_view!(
	bool, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, Decimal, Date, DateTime, Time, Duration
);

pub struct InProcessCursor<'c, T> {
	view: &'c ColumnView<'c>,
	rows: usize,
	_value: PhantomData<fn() -> T>,
}

impl<'c, T: ViewCell<'c>> ColumnCursor<T> for InProcessCursor<'c, T> {
	fn get(&self, index: usize) -> Result<Option<T>, SdkError> {
		if index >= self.rows {
			return Err(SdkError::InvalidInput(format!(
				"row {index} is out of range for column {} with {} rows",
				self.view.field.name(),
				self.rows
			)));
		}
		if !self.view.is_defined(index) {
			return Ok(None);
		}
		T::read_view(self.view, index)
	}
}

fn column_read<T: ?Sized>(name: &str, view: &ColumnView<'_>, reason: ColumnReadReason) -> SdkError {
	SdkError::ColumnRead {
		column: name.to_string(),
		column_type: view.base_type(),
		target: type_name::<T>(),
		reason,
	}
}

pub struct InProcessColumnsView<'a> {
	columns: Arc<ResolvedColumns<'a>>,
}

impl<'a> InProcessColumnsView<'a> {
	pub fn new(batch: &'a RecordBatch) -> Self {
		Self {
			columns: Arc::new(ResolvedColumns::new(batch)),
		}
	}
}

impl<'a> ColumnsView for InProcessColumnsView<'a> {
	fn row_count(&self) -> usize {
		self.columns.batch.num_rows()
	}

	fn row(&self, index: usize) -> Option<impl RowView + '_> {
		if index >= self.columns.batch.num_rows() {
			return None;
		}
		Some(InProcessRowView {
			columns: Arc::clone(&self.columns),
			index,
		})
	}

	fn column<'c, T: CursorValue<'c> + 'c>(
		&'c self,
		name: &str,
	) -> Result<Option<impl ColumnCursor<T> + 'c>, SdkError> {
		Ok(self.columns.view(name)?.map(|view| InProcessCursor {
			view,
			rows: self.columns.batch.num_rows(),
			_value: PhantomData,
		}))
	}

	fn row_numbers(&self) -> &[RowNumber] {
		self.columns.row_numbers()
	}

	fn times(&self) -> RowTimes<'_> {
		self.columns.times()
	}
}

pub struct InProcessDiffView<'a> {
	diff: &'a Diff,
}

impl<'a> InProcessDiffView<'a> {
	pub fn new(diff: &'a Diff) -> Self {
		Self {
			diff,
		}
	}
}

impl<'a> DiffView for InProcessDiffView<'a> {
	fn kind(&self) -> DiffType {
		self.diff.kind()
	}

	fn pre(&self) -> Option<impl ColumnsView + '_> {
		self.diff.pre().map(InProcessColumnsView::new)
	}

	fn post(&self) -> Option<impl ColumnsView + '_> {
		self.diff.post().map(InProcessColumnsView::new)
	}
}

pub struct InProcessChangeView<'a> {
	change: &'a Change,
}

impl<'a> InProcessChangeView<'a> {
	pub fn new(change: &'a Change) -> Self {
		Self {
			change,
		}
	}
}

impl<'a> ChangeView for InProcessChangeView<'a> {
	fn version(&self) -> u64 {
		self.change.version.source.0
	}

	fn changed_at_nanos(&self) -> i64 {
		self.change.changed_at.to_nanos()
	}

	fn diff_count(&self) -> usize {
		self.change.diffs.len()
	}

	fn diff(&self, index: usize) -> Option<impl DiffView + '_> {
		self.change.diffs.get(index).map(InProcessDiffView::new)
	}
}
