// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::ValueError, value::value_type::ValueType};

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

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
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<(), ValueError> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>, ValueError> {
		if self.exhausted {
			return Ok(None);
		}

		let roles = CatalogStore::list_all_roles(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, roles.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, roles.len());

		for r in roles {
			ids.push(r.id);
			names.push(r.name.as_str());
		}

		let columns = vec![ids.finish("id"), names.finish("name")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
