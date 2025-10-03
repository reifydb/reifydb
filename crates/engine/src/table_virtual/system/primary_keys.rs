// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system primary key information
pub struct PrimaryKeys {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl PrimaryKeys {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_primary_keys_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<'a> TableVirtual<'a> for PrimaryKeys {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut pk_ids = Vec::new();
		let mut source_ids = Vec::new();

		// Read primary keys from storage instead of in-memory catalog
		let primary_keys = CatalogStore::list_primary_keys(txn)?;
		for pk_info in primary_keys {
			pk_ids.push(pk_info.def.id.0);
			source_ids.push(pk_info.source_id);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("id"),
				data: ColumnData::uint8(pk_ids),
			},
			Column {
				name: Fragment::owned_internal("source_id"),
				data: ColumnData::uint8(source_ids),
			},
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
