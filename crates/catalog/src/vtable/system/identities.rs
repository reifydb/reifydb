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

/// Virtual table that exposes system identity information
pub struct SystemIdentities {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemIdentities {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemIdentities {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_identities_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemIdentities {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let identities = CatalogStore::list_all_identities(txn)?;

		let mut ids = ColumnBuffer::identity_id_with_capacity(identities.len());
		let mut names = ColumnBuffer::utf8_with_capacity(identities.len());
		let mut enabled_flags = ColumnBuffer::bool_with_capacity(identities.len());

		for u in identities {
			ids.push(u.id);
			names.push(u.name.as_str());
			enabled_flags.push(u.enabled);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("enabled"), enabled_flags),
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
