// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system primary key information
pub struct PrimaryKeys {
	pub(crate) definition: Arc<VTableDef>,
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

impl VTable for PrimaryKeys {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
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
				name: Fragment::internal("id"),
				data: ColumnData::uint8(pk_ids),
			},
			Column {
				name: Fragment::internal("source_id"),
				data: ColumnData::uint8(source_ids),
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
