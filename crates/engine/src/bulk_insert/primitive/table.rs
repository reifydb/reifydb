// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::params::Params;

use crate::bulk_insert::builder::{BulkInsertBuilder, ValidationMode};

#[derive(Debug, Clone)]
pub struct PendingTableInsert {
	pub namespace: String,
	pub table: String,
	pub rows: Vec<Params>,
}

impl PendingTableInsert {
	pub fn new(namespace: String, table: String) -> Self {
		Self {
			namespace,
			table,
			rows: Vec::new(),
		}
	}

	pub fn add_row(&mut self, params: Params) {
		self.rows.push(params);
	}

	pub fn add_rows<I: IntoIterator<Item = Params>>(&mut self, iter: I) {
		self.rows.extend(iter);
	}
}

pub struct TableInsertBuilder<'a, 'e, V: ValidationMode> {
	parent: &'a mut BulkInsertBuilder<'e, V>,
	pending: PendingTableInsert,
}

impl<'a, 'e, V: ValidationMode> TableInsertBuilder<'a, 'e, V> {
	pub(crate) fn new(parent: &'a mut BulkInsertBuilder<'e, V>, namespace: String, table: String) -> Self {
		Self {
			parent,
			pending: PendingTableInsert::new(namespace, table),
		}
	}

	pub fn row(mut self, params: Params) -> Self {
		self.pending.add_row(params);
		self
	}

	pub fn rows<I>(mut self, iter: I) -> Self
	where
		I: IntoIterator<Item = Params>,
	{
		self.pending.add_rows(iter);
		self
	}

	pub fn done(self) -> &'a mut BulkInsertBuilder<'e, V> {
		self.parent.add_table_insert(self.pending);
		self.parent
	}
}
