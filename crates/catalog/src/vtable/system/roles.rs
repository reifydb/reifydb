// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system role information
pub struct SystemRoles {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemRoles {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemRoles {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_roles_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemRoles {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let roles = CatalogStore::list_all_roles(txn)?;

		let mut ids = ColumnBuffer::uint8_with_capacity(roles.len());
		let mut names = ColumnBuffer::utf8_with_capacity(roles.len());

		for r in roles {
			ids.push(r.id);
			names.push(r.name.as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("name"), names),
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
