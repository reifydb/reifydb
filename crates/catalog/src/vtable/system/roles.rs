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
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system role information
pub struct Roles {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Roles {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_roles_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for Roles {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let roles = CatalogStore::list_all_roles(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(roles.len());
		let mut names = ColumnData::utf8_with_capacity(roles.len());

		for r in roles {
			ids.push(r.id);
			names.push(r.name.as_str());
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
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
