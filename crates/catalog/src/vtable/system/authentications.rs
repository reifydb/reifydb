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

/// Virtual table that exposes system authentication information
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

		let mut ids = ColumnData::uint8_with_capacity(auths.len());
		let mut identities = ColumnData::identity_id_with_capacity(auths.len());
		let mut methods = ColumnData::utf8_with_capacity(auths.len());

		for a in auths {
			ids.push(a.id);
			identities.push(a.identity);
			methods.push(a.method.as_str());
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("identity"),
				data: identities,
			},
			Column {
				name: Fragment::internal("method"),
				data: methods,
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
