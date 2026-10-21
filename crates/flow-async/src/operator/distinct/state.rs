// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashSet},
	mem::size_of,
	sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use indexmap::{IndexMap, IndexSet};
use reifydb_codec::row::{
	bytes::EncodedBytes,
	pod::EncodedPodRowBuilder,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::{
	key::operator::state::GroupId,
	metrics::heap::HeapSize,
	value::{
		batch::{batch, decode_cells},
		column::builder::ColumnBuilder,
	},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	util::{cowvec::CowVec, hash::Hash128},
	value::{
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

use crate::operator::row_times;

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
	width: u32,

	#[serde(with = "serde_bytes")]
	body: Vec<u8>,
}

impl HeapSize for SerializedRow {
	fn heap_size(&self) -> usize {
		self.body.capacity()
	}
}

impl DistinctLayout {
	pub(super) fn new() -> Self {
		Self {
			names: Vec::new(),
			types: Vec::new(),
		}
	}

	fn shape(&self, width: usize) -> RowShape {
		let fields = self.names[..width]
			.iter()
			.zip(&self.types[..width])
			.map(|(name, typ)| RowShapeField::unconstrained(name.clone(), typ.clone()))
			.collect();
		RowShape::new(RowFamily::Pod, fields)
	}

	pub(super) fn encode(&self, columns: &RecordBatch) -> Result<Vec<SerializedRow>> {
		let views = user_views(columns)?;
		let shape = self.shape(views.len());
		let mut bodies: Vec<EncodedPodRowBuilder> =
			(0..columns.num_rows()).map(|_| shape.allocate_pod()).collect();
		shape.write_columns(&mut bodies, &views)?;
		let numbers = require_row_numbers(columns)?;
		let created = created_at(columns)?;
		let updated = updated_at(columns)?;
		let times = row_times(columns)?;
		Ok(bodies
			.into_iter()
			.enumerate()
			.map(|(row, body)| SerializedRow {
				number: numbers[row],
				created_at: created.get(row).copied().unwrap_or_default(),
				updated_at: updated.get(row).copied().unwrap_or_default(),
				time: times.get(row).copied().flatten().unwrap_or_default(),
				width: views.len() as u32,
				body: body.freeze().as_slice().to_vec(),
			})
			.collect())
	}

	pub(super) fn decode(&self, rows: &[SerializedRow]) -> Result<RecordBatch> {
		let mut runs: Vec<(RowShape, Vec<EncodedBytes>)> = Vec::new();
		for row in rows {
			let width = row.width as usize;
			if runs.last().is_none_or(|(shape, _)| shape.field_count() != width) {
				runs.push((self.shape(width), Vec::new()));
			}
			runs.last_mut().unwrap().1.push(EncodedBytes(CowVec::new(row.body.clone())));
		}

		let mut columns_vec = Vec::with_capacity(self.names.len());
		for (index, (name, typ)) in self.names.iter().zip(&self.types).enumerate() {
			let mut builder = ColumnBuilder::with_capacity(typ.clone(), rows.len());
			for (shape, bodies) in &runs {
				if index < shape.field_count() {
					decode_cells(&mut builder, name, shape, index, bodies)?;
				} else {
					bodies.iter().for_each(|_| builder.push_none());
				}
			}
			columns_vec.push(builder.finish(name));
		}

		let stamps: [(SystemColumn, ArrayRef); 4] = [
			(
				SystemColumn::RowNumbers,
				Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.number.0))),
			),
			(SystemColumn::CreatedAt, Arc::new(datetime_array(rows.iter().map(|row| row.created_at)))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array(rows.iter().map(|row| row.updated_at)))),
			(SystemColumn::Time, Arc::new(datetime_array(rows.iter().map(|row| row.time)))),
		];
		stamps.into_iter().try_fold(batch(columns_vec)?, |columns, (column, array)| {
			with_system_column(columns, column, array)
		})
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

	pub(super) dirty: HashSet<(Hash128, RowNumber)>,

	pub(super) loaded: HashSet<(Hash128, RowNumber)>,

	pub(super) layout_changed: bool,

	pub(super) withdrawn: IndexSet<GroupId>,
}

impl Default for DistinctState {
	fn default() -> Self {
		Self {
			entries: IndexMap::new(),
			layout: DistinctLayout::new(),
			dirty: HashSet::new(),
			loaded: HashSet::new(),
			layout_changed: false,
			withdrawn: IndexSet::new(),
		}
	}
}

impl HeapSize for DistinctState {
	fn heap_size(&self) -> usize {
		self.entries.capacity() * (size_of::<Hash128>() + size_of::<DistinctEntry>())
			+ self.entries.values().map(HeapSize::heap_size).sum::<usize>()
			+ self.layout.heap_size()
			+ self.dirty.heap_size()
			+ self.loaded.heap_size()
			+ self.withdrawn.capacity() * size_of::<GroupId>()
	}
}
