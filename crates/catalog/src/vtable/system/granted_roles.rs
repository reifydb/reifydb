// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemGrantedRoles {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemGrantedRoles {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemGrantedRoles {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_granted_roles_table().clone(),
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

		let mut identities = ColumnBuilder::with_capacity(ValueType::IdentityId, granted_roles.len());
		let mut role_ids = ColumnBuilder::with_capacity(ValueType::Uint8, granted_roles.len());

		for ir in granted_roles {
			identities.push(ir.identity);
			role_ids.push(ir.role_id);
		}

		let columns = vec![identities.finish("identity"), role_ids.finish("role_id")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
