// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system schema information
pub struct Schemas<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Schemas<T> {
	pub fn new() -> Self {
		Self {
			definition:
				SystemCatalog::get_system_schemas_table_def()
					.clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for Schemas<T> {
	fn initialize(
		&mut self,
		_txn: &mut StandardTransaction<'a, T>,
		_ctx: TableVirtualContext<'a>,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(
		&mut self,
		txn: &mut StandardTransaction<'a, T>,
	) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut schema_ids = Vec::new();
		let mut schema_names = Vec::new();

		let schemas = CatalogStore::list_schemas_all(txn)?;
		for schema in schemas {
			schema_ids.push(schema.id.0);
			schema_names.push(schema.name);
		}

		let columns = vec![
			Column::ColumnQualified(ColumnQualified {
				name: "id".to_string(),
				data: ColumnData::uint8(schema_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "name".to_string(),
				data: ColumnData::utf8(schema_names),
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
