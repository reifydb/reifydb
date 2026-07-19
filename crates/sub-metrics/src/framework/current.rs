// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::fragment::Fragment;

#[derive(Clone)]
pub struct CurrentCache {
	columns: Vec<UserVTableColumn>,
	data: Arc<RwLock<Columns>>,
}

impl CurrentCache {
	pub fn new(columns: Vec<UserVTableColumn>) -> Self {
		let empty = empty_columns(&columns);
		Self {
			columns,
			data: Arc::new(RwLock::new(empty)),
		}
	}

	pub fn store(&self, columns: Columns) {
		*self.data.write() = columns;
	}

	pub fn load(&self) -> Columns {
		self.data.read().clone()
	}

	pub fn columns(&self) -> Vec<UserVTableColumn> {
		self.columns.clone()
	}
}

fn empty_columns(columns: &[UserVTableColumn]) -> Columns {
	Columns::new(
		columns.iter()
			.map(|c| {
				ColumnWithName::new(
					Fragment::internal(c.name.clone()),
					ColumnBuffer::with_capacity(c.data_type.clone(), 0),
				)
			})
			.collect(),
	)
}

#[derive(Clone)]
pub struct CurrentVTable {
	cache: CurrentCache,
}

impl CurrentVTable {
	pub fn new(cache: CurrentCache) -> Self {
		Self {
			cache,
		}
	}
}

impl UserVTable for CurrentVTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		self.cache.columns()
	}

	fn get(&self) -> Columns {
		self.cache.load()
	}
}
