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

/// Virtual table that exposes system identity-role assignment information
pub struct SystemGrantedRoles {
	pub(crate) definition: Arc<VTable>,
	exhausted: bool,
}

impl SystemGrantedRoles {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_granted_roles_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemGrantedRoles {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let granted_roles = CatalogStore::list_all_granted_roles(txn)?;

		let mut identities = ColumnData::identity_id_with_capacity(granted_roles.len());
		let mut role_ids = ColumnData::uint8_with_capacity(granted_roles.len());

		for ir in granted_roles {
			identities.push(ir.identity);
			role_ids.push(ir.role_id);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("identity"),
				data: identities,
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

	fn definition(&self) -> &VTable {
		&self.definition
	}
}
