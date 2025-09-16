// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};
use reifydb_type::Value;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system table information
pub struct Tables<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Tables<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_tables_table_def().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for Tables<T> {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a, T>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> Result<Option<Batch>> {
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
			Column::ColumnQualified(ColumnQualified {
				name: "id".to_string(),
				data: ids,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "namespace_id".to_string(),
				data: namespaces,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "name".to_string(),
				data: names,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "primary_key_id".to_string(),
				data: primary_keys,
			}),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
