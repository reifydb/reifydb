// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::type_name,
	collections::HashMap,
	sync::{Arc, OnceLock},
};

use arrow_array::RecordBatch;
use reifydb_core::interface::change::{Change, Diff};
use reifydb_value::{
	error::ColumnReadReason,
	value::{
		Value,
		column_view::{ColumnView, FromColumnView, ViewData},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		diff_type::DiffType,
		duration::Duration,
		row_number::RowNumber,
		system_columns::{is_system_field, row_numbers, time},
		time::Time,
	},
};

use super::{ChangeView, ColumnsView, DiffView, RowView};
use crate::error::SdkError;

struct ResolvedColumns<'a> {
	batch: &'a RecordBatch,
	positions: HashMap<&'a str, usize>,
	views: Box<[OnceLock<Option<ColumnView<'a>>>]>,
	time: OnceLock<&'a [DateTime]>,
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
			time: OnceLock::new(),
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
		let view = ColumnView::try_from((batch.column(index), batch.schema_ref().field(index)))?;
		Ok(slot.get_or_init(|| Some(view).filter(|view| !is_system_field(view.field))).as_ref())
	}

	fn time(&self) -> &'a [DateTime] {
		self.time.get_or_init(|| {
			time(self.batch).unwrap_or_else(|e| panic!("in-process #time column does not read: {e}"))
		})
	}
}

pub struct InProcessRowView<'a> {
	columns: Arc<ResolvedColumns<'a>>,
	index: usize,
}

impl<'a> InProcessRowView<'a> {
	pub fn new(batch: &'a RecordBatch, index: usize) -> Self {
		Self {
			columns: Arc::new(ResolvedColumns::new(batch)),
			index,
		}
	}

	fn buffer(&self, name: &str) -> Result<Option<&ColumnView<'a>>, SdkError> {
		self.columns.view(name)
	}

	fn readable(&self, name: &str) -> Option<&ColumnView<'a>> {
		self.buffer(name).unwrap_or_else(|e| panic!("in-process column '{name}' does not read: {e}"))
	}

	fn defined(&self, name: &str) -> Result<Option<&ColumnView<'a>>, SdkError> {
		Ok(self.buffer(name)?.filter(|view| view.is_defined(self.index)))
	}

	fn typed<T: FromColumnView>(&self, name: &str) -> Result<Option<T>, SdkError> {
		let Some(view) = self.defined(name)? else {
			return Ok(None);
		};
		T::from_column_view(view, self.index).map_err(|reason| column_read::<T>(name, view, reason))
	}
}

impl<'a> RowView for InProcessRowView<'a> {
	fn is_defined(&self, name: &str) -> bool {
		self.readable(name).map(|view| view.is_defined(self.index)).unwrap_or(false)
	}

	fn utf8(&self, name: &str) -> Result<Option<&str>, SdkError> {
		let Some(view) = self.defined(name)? else {
			return Ok(None);
		};
		if !matches!(view.data, ViewData::Utf8 { .. }) {
			return Err(column_read::<&str>(name, view, ColumnReadReason::WrongType));
		}
		Ok(view.get_str(self.index))
	}

	fn blob(&self, name: &str) -> Result<Option<&[u8]>, SdkError> {
		let Some(view) = self.defined(name)? else {
			return Ok(None);
		};
		if !matches!(view.data, ViewData::Blob { .. }) {
			return Err(column_read::<&[u8]>(name, view, ColumnReadReason::WrongType));
		}
		Ok(view.get_bytes(self.index))
	}

	fn bool(&self, name: &str) -> Result<Option<bool>, SdkError> {
		self.typed(name)
	}

	fn u8(&self, name: &str) -> Result<Option<u8>, SdkError> {
		self.typed(name)
	}

	fn u16(&self, name: &str) -> Result<Option<u16>, SdkError> {
		self.typed(name)
	}

	fn u32(&self, name: &str) -> Result<Option<u32>, SdkError> {
		self.typed(name)
	}

	fn u64(&self, name: &str) -> Result<Option<u64>, SdkError> {
		self.typed(name)
	}

	fn u128(&self, name: &str) -> Result<Option<u128>, SdkError> {
		self.typed(name)
	}

	fn i8(&self, name: &str) -> Result<Option<i8>, SdkError> {
		self.typed(name)
	}

	fn i16(&self, name: &str) -> Result<Option<i16>, SdkError> {
		self.typed(name)
	}

	fn i32(&self, name: &str) -> Result<Option<i32>, SdkError> {
		self.typed(name)
	}

	fn i64(&self, name: &str) -> Result<Option<i64>, SdkError> {
		self.typed(name)
	}

	fn i128(&self, name: &str) -> Result<Option<i128>, SdkError> {
		self.typed(name)
	}

	fn f32(&self, name: &str) -> Result<Option<f32>, SdkError> {
		self.typed(name)
	}

	fn f64(&self, name: &str) -> Result<Option<f64>, SdkError> {
		self.typed(name)
	}

	fn decimal(&self, name: &str) -> Result<Option<Decimal>, SdkError> {
		self.typed(name)
	}

	fn date(&self, name: &str) -> Result<Option<Date>, SdkError> {
		self.typed(name)
	}

	fn datetime(&self, name: &str) -> Result<Option<DateTime>, SdkError> {
		self.typed(name)
	}

	fn time(&self, name: &str) -> Result<Option<Time>, SdkError> {
		self.typed(name)
	}

	fn duration(&self, name: &str) -> Result<Option<Duration>, SdkError> {
		self.typed(name)
	}

	fn value(&self, name: &str) -> Option<Value> {
		self.readable(name).map(|view| view.get_value(self.index))
	}

	fn row_number(&self) -> Option<RowNumber> {
		row_numbers(self.columns.batch)
			.unwrap_or_else(|e| panic!("in-process #rownum column does not read: {e}"))
			.get(self.index)
			.copied()
	}

	fn row_time(&self) -> Option<DateTime> {
		self.columns.time().get(self.index).copied()
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
