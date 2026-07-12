// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, sumtype::SumTypeId, value_type::ValueType};
use serde::{Deserialize, Serialize};

use crate::{
	interface::catalog::{
		column::Column,
		id::{NamespaceId, SeriesId},
		key::{KeySpec, PrimaryKey},
	},
	value::column::buffer::ColumnBuffer,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Series {
	pub id: SeriesId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub tag: Option<SumTypeId>,
	pub key: KeySpec,
	pub primary_key: Option<PrimaryKey>,
	pub partition_by: Vec<String>,
	pub underlying: bool,
}

impl Series {
	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn key_column_type(&self) -> Option<ValueType> {
		let key_col_name = self.key.column();
		self.columns.iter().find(|c| c.name == key_col_name).map(|c| c.constraint.get_type())
	}

	pub fn key_to_u64(&self, value: Value) -> Option<u64> {
		self.key.value_to_u64(value)
	}

	pub fn key_from_u64(&self, v: u64) -> Value {
		self.key.value_from_u64(self.key_column_type(), v)
	}

	pub fn key_column_data(&self, keys: Vec<u64>) -> ColumnBuffer {
		let key_type = self.key_column_type();
		match &key_type {
			Some(ty) => {
				let mut data = ColumnBuffer::with_capacity(ty.clone(), keys.len());
				for k in keys {
					data.push_value(self.key_from_u64(k));
				}
				data
			}
			None => ColumnBuffer::uint8(keys),
		}
	}

	pub fn data_columns(&self) -> impl Iterator<Item = &Column> {
		let key_column = self.key.column().to_string();
		self.columns.iter().filter(move |c| c.name != key_column)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesMetadata {
	pub id: SeriesId,
	pub row_count: u64,
	pub oldest_key: u64,
	pub newest_key: u64,
	pub sequence_counter: u64,
}

impl SeriesMetadata {
	pub fn new(series_id: SeriesId) -> Self {
		Self {
			id: series_id,
			row_count: 0,
			oldest_key: 0,
			newest_key: 0,
			sequence_counter: 0,
		}
	}
}
