// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system primary key information
pub struct SystemPrimaryKeys {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemPrimaryKeys {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemPrimaryKeys {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_primary_keys_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemPrimaryKeys {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut pk_ids = Vec::new();
		let mut shape_ids = Vec::new();

		// Read primary keys from storage instead of in-memory catalog
		let primary_keys = CatalogStore::list_primary_keys(txn)?;
		for pk_info in primary_keys {
			pk_ids.push(pk_info.def.id.0);
			shape_ids.push(pk_info.shape_id);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(pk_ids),
			},
			Column {
				name: Fragment::internal("shape_id"),
				data: ColumnData::uint8(shape_ids),
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
