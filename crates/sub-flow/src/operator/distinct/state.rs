// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashSet},
	mem::size_of,
};

use indexmap::IndexMap;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	metrics::heap::HeapSize,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	fragment::Fragment,
	util::hash::Hash128,
	value::{
		Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType,
	},
};
use serde::{Deserialize, Serialize};

#[operator_state]
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
	pub(super) fn from_columns_at_index(columns: &Columns, row_idx: usize) -> Self {
		let number = columns.row_numbers()[row_idx];
		let created_at = if columns.created_at().is_empty() {
			DateTime::default()
		} else {
			columns.created_at()[row_idx]
		};
		let updated_at = if columns.updated_at().is_empty() {
			DateTime::default()
		} else {
			columns.updated_at()[row_idx]
		};
		let time = if columns.time().is_empty() {
			DateTime::default()
		} else {
			columns.time()[row_idx]
		};

		let values: Vec<Value> = columns.iter().map(|c| c.data().get_value(row_idx)).collect();

		let values_bytes = to_stdvec(&values).expect("Failed to serialize column values");

		Self {
			number,
			created_at,
			updated_at,
			time,
			values_bytes,
		}
	}

	pub(super) fn to_columns(&self, layout: &DistinctLayout) -> Columns {
		let values: Vec<Value> = from_bytes(&self.values_bytes).expect("Failed to deserialize column values");

		let mut columns_vec = Vec::with_capacity(layout.names.len());
		for (i, (name, typ)) in layout.names.iter().zip(layout.types.iter()).enumerate() {
			let value = values.get(i).cloned().unwrap_or(Value::none());
			let mut col_data = ColumnBuffer::with_capacity(typ.clone(), 1);
			col_data.push_value(value);
			columns_vec.push(ColumnWithName::new(Fragment::internal(name), col_data));
		}

		Columns::with_system(
			columns_vec,
			SystemColumns::new(
				vec![self.number],
				Vec::new(),
				vec![self.created_at],
				vec![self.updated_at],
				vec![self.time],
			),
		)
	}
}

impl DistinctLayout {
	pub(super) fn new() -> Self {
		Self {
			names: Vec::new(),
			types: Vec::new(),
		}
	}

	pub(super) fn update_from_columns(&mut self, columns: &Columns) -> bool {
		if columns.is_empty() {
			return false;
		}

		let names: Vec<String> = columns.iter().map(|c| c.name().text().to_string()).collect();
		let types: Vec<ValueType> = columns.iter().map(|c| c.data().get_type()).collect();

		if self.names.is_empty() {
			self.names = names;
			self.types = types;
			return true;
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
		changed
	}
}

#[operator_state]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DistinctEntry {
	pub(super) rows: BTreeMap<RowNumber, SerializedRow>,

	pub(super) last_seen_nanos: u64,
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

	pub(super) dirty: HashSet<Hash128>,

	pub(super) layout_dirty: bool,
}

impl Default for DistinctState {
	fn default() -> Self {
		Self {
			entries: IndexMap::new(),
			layout: DistinctLayout::new(),
			dirty: HashSet::new(),
			layout_dirty: false,
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
