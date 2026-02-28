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

/// Virtual table that exposes system user-role assignment information
pub struct UserRoles {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl UserRoles {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_user_roles_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for UserRoles {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let user_roles = CatalogStore::list_all_user_roles(txn)?;

		let mut user_ids = ColumnData::uint8_with_capacity(user_roles.len());
		let mut role_ids = ColumnData::uint8_with_capacity(user_roles.len());

		for ur in user_roles {
			user_ids.push(ur.user_id);
			role_ids.push(ur.role_id);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("user_id"),
				data: user_ids,
			},
			Column {
				name: Fragment::internal("role_id"),
				data: role_ids,
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
