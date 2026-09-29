// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, value_type::ValueType};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemTables {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemTables {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemTables {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_tables_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemTables {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let tables = CatalogStore::list_tables(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, tables.len());
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Uint8, tables.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, tables.len());
		let mut primary_keys = ColumnBuilder::with_capacity(ValueType::Uint8, tables.len());
		let mut times = ColumnBuilder::with_capacity(ValueType::Utf8, tables.len());
		let mut timestamps = ColumnBuilder::with_capacity(ValueType::Utf8, tables.len());

		for table in tables {
			ids.push(table.id.0);
			namespaces.push(table.namespace.0);
			names.push(table.name.as_str());
			primary_keys.push_value(
				table.primary_key
					.map(|pk| pk.id.0)
					.map(Value::Uint8)
					.unwrap_or(Value::none_of(ValueType::Uint8)),
			);
			times.push(table.time.domain().as_str());
			timestamps.push(table.time.ts().unwrap_or_default());
		}

		let columns = vec![
			ids.finish("id"),
			namespaces.finish("namespace_id"),
			names.finish("name"),
			primary_keys.finish("primary_key_id"),
			times.finish("time"),
			timestamps.finish("ts"),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
