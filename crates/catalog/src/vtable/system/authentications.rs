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

pub struct SystemAuthentications {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemAuthentications {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemAuthentications {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_authentications_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemAuthentications {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let auths = CatalogStore::list_all_authentications(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, auths.len());
		let mut identities = ColumnBuilder::with_capacity(ValueType::IdentityId, auths.len());
		let mut methods = ColumnBuilder::with_capacity(ValueType::Utf8, auths.len());

		for a in auths {
			ids.push(a.id);
			identities.push(a.identity);
			methods.push(a.method.as_str());
		}

		let columns = vec![ids.finish("id"), identities.finish("identity"), methods.finish("method")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
