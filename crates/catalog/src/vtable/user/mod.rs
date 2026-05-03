// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod builder;
pub mod registry;

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::{Value, r#type::Type};

use crate::Result;

#[derive(Debug, Clone)]
pub struct UserVTableColumn {
	pub name: String,

	pub data_type: Type,

	pub undefined: bool,
}

impl UserVTableColumn {
	pub fn new(name: impl Into<String>, data_type: Type) -> Self {
		Self {
			name: name.into(),
			data_type,
			undefined: false,
		}
	}
}

pub trait UserVTable: Clone + Send + Sync + 'static {
	fn vtable(&self) -> Vec<UserVTableColumn>;

	fn get(&self) -> Columns;
}

#[derive(Debug, Clone, Default)]
pub struct UserVTablePushdownContext {
	pub limit: Option<usize>,
}

pub trait UserVTableIterator: Send + Sync + 'static {
	fn columns(&self) -> Vec<UserVTableColumn>;

	fn initialize(&mut self, ctx: Option<&UserVTablePushdownContext>) -> Result<()>;

	fn next_batch(&mut self, batch_size: usize) -> Result<Option<Vec<Vec<Value>>>>;
}
