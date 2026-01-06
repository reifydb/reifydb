// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::VTableDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{Fragment, Value};

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system table information
pub struct Tables {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Tables {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_tables_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: IntoStandardTransaction> VTable<T> for Tables {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let tables = CatalogStore::list_tables_all(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(tables.len());
		let mut namespaces = ColumnData::uint8_with_capacity(tables.len());
		let mut names = ColumnData::utf8_with_capacity(tables.len());
		let mut primary_keys = ColumnData::uint4_with_capacity(tables.len());

		for table in tables {
			ids.push(table.id.0);
			namespaces.push(table.namespace.0);
			names.push(table.name.as_str());
			primary_keys.push_value(
				table.primary_key.map(|pk| pk.id.0).map(Value::Uint8).unwrap_or(Value::Undefined),
			);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespaces,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("primary_key_id"),
				data: primary_keys,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
