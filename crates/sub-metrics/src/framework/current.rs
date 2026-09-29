// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{FieldRef, Schema};
use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::value::column::builder::ColumnBuilder;
use reifydb_runtime::sync::rwlock::RwLock;

#[derive(Clone)]
pub struct CurrentCache {
	columns: Vec<UserVTableColumn>,
	data: Arc<RwLock<RecordBatch>>,
}

impl CurrentCache {
	pub fn new(columns: Vec<UserVTableColumn>) -> Self {
		let empty = empty_columns(&columns);
		Self {
			columns,
			data: Arc::new(RwLock::new(empty)),
		}
	}

	pub fn store(&self, columns: RecordBatch) {
		*self.data.write() = columns;
	}

	pub fn load(&self) -> RecordBatch {
		self.data.read().clone()
	}

	pub fn columns(&self) -> Vec<UserVTableColumn> {
		self.columns.clone()
	}
}

fn empty_columns(columns: &[UserVTableColumn]) -> RecordBatch {
	let fields: Vec<FieldRef> = columns
		.iter()
		.map(|c| ColumnBuilder::with_capacity(c.data_type.clone(), 0).finish(&c.name).0)
		.collect();
	RecordBatch::new_empty(Arc::new(Schema::new(fields)))
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

	fn get(&self) -> RecordBatch {
		self.cache.load()
	}
}
