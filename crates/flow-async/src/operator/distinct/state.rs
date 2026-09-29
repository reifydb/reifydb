// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	mem::size_of,
	sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use indexmap::IndexMap;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	metrics::heap::HeapSize,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{
		Value,
		column_view::ColumnView,
		container::temporal_array::datetime_array,
		datetime::DateTime,
		row_number::RowNumber,
		system_columns::{
			SystemColumn, created_at, require_row_numbers, updated_at, user_columns, with_system_column,
		},
		value_type::ValueType,
	},
};
use serde::{Deserialize, Serialize};

use crate::operator::time_at;

pub(super) fn user_views(columns: &RecordBatch) -> Result<Vec<ColumnView<'_>>> {
	user_columns(columns).map(|(field, array)| ColumnView::try_from((array, field.as_ref()))).collect()
}

#[operator_state]
#[derive(Debug, Clone)]
pub(super) struct DistinctLayout {
	names: Vec<String>,
	types: Vec<ValueType>,
}

impl HeapSize for DistinctLayout {
	fn heap_size(&self) -> usize {
		self.names.heap_size() + self.types.capacity() * size_of::<ValueType>()
	}
}

#[operator_state]
#[derive(Debug, Clone)]
pub(super) struct SerializedRow {
	number: RowNumber,
	created_at: DateTime,
	updated_at: DateTime,
	time: DateTime,

	#[serde(with = "serde_bytes")]
	values_bytes: Vec<u8>,
}

impl HeapSize for SerializedRow {
	fn heap_size(&self) -> usize {
		self.values_bytes.capacity()
	}
}

impl SerializedRow {
	pub(super) fn from_columns_at_index(columns: &RecordBatch, row_idx: usize) -> Result<Self> {
		let number = require_row_numbers(columns)?[row_idx];
		let created_at = created_at(columns)?.get(row_idx).copied().unwrap_or_default();
		let updated_at = updated_at(columns)?.get(row_idx).copied().unwrap_or_default();
		let time = time_at(columns, row_idx)?.unwrap_or_default();

		let values: Vec<Value> = user_views(columns)?.iter().map(|view| view.get_value(row_idx)).collect();

		let values_bytes = to_stdvec(&values).expect("Failed to serialize column values");

		Ok(Self {
			number,
			created_at,
			updated_at,
			time,
			values_bytes,
		})
	}

	pub(super) fn to_columns(&self, layout: &DistinctLayout) -> Result<RecordBatch> {
		let values: Vec<Value> = from_bytes(&self.values_bytes).expect("Failed to deserialize column values");

		let mut columns_vec = Vec::with_capacity(layout.names.len());
		for (i, (name, typ)) in layout.names.iter().zip(layout.types.iter()).enumerate() {
			let value = values.get(i).cloned().unwrap_or(Value::none());
			let mut col_data = ColumnBuilder::with_capacity(typ.clone(), 1);
			col_data.push_value(value);
			columns_vec.push(col_data.finish(name));
		}

		let stamps: [(SystemColumn, ArrayRef); 4] = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![self.number.0]))),
			(SystemColumn::CreatedAt, Arc::new(datetime_array([self.created_at]))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array([self.updated_at]))),
			(SystemColumn::Time, Arc::new(datetime_array([self.time]))),
		];
		stamps.into_iter().try_fold(batch(columns_vec)?, |columns, (column, array)| {
			with_system_column(columns, column, array)
		})
	}
}

impl DistinctLayout {
	pub(super) fn new() -> Self {
		Self {
			names: Vec::new(),
			types: Vec::new(),
		}
	}

	pub(super) fn update_from_columns(&mut self, columns: &RecordBatch) -> Result<bool> {
		let views = user_views(columns)?;
		if views.is_empty() {
			return Ok(false);
		}

		let names: Vec<String> = views.iter().map(|view| view.field.name().clone()).collect();
		let types: Vec<ValueType> = views.iter().map(|view| view.get_type()).collect();

		if self.names.is_empty() {
			self.names = names;
			self.types = types;
			return Ok(true);
		}

		let mut changed = false;
		for (i, new_type) in types.iter().enumerate() {
			if i < self.types.len() {
				if !self.types[i].is_option() && new_type.is_option() {
					self.types[i] = new_type.clone();
					changed = true;
				}
			} else {
				self.types.push(new_type.clone());
				if i < names.len() {
					self.names.push(names[i].clone());
				}
				changed = true;
			}
		}
		Ok(changed)
	}
}

#[operator_state]
#[derive(Debug, Clone)]
pub(super) struct DistinctEntry {
	pub(super) rows: BTreeMap<RowNumber, SerializedRow>,
}

impl HeapSize for DistinctEntry {
	fn heap_size(&self) -> usize {
		self.rows.heap_size()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DistinctState {
	pub(super) entries: IndexMap<Hash128, DistinctEntry>,

	pub(super) layout: DistinctLayout,

	pub(super) dirty: HashMap<Hash128, DateTime>,

	pub(super) layout_changed_at: Option<DateTime>,
}

impl Default for DistinctState {
	fn default() -> Self {
		Self {
			entries: IndexMap::new(),
			layout: DistinctLayout::new(),
			dirty: HashMap::new(),
			layout_changed_at: None,
		}
	}
}

impl HeapSize for DistinctState {
	fn heap_size(&self) -> usize {
		self.entries.capacity() * (size_of::<Hash128>() + size_of::<DistinctEntry>())
			+ self.entries.values().map(HeapSize::heap_size).sum::<usize>()
			+ self.layout.heap_size()
			+ self.dirty.heap_size()
	}
}
