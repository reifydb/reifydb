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

/// Virtual table that exposes system identity information
pub struct SystemIdentities {
	pub(crate) definition: Arc<VTable>,
	exhausted: bool,
}

impl SystemIdentities {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_identities_table().clone(),
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

		let mut ids = ColumnData::identity_id_with_capacity(identities.len());
		let mut names = ColumnData::utf8_with_capacity(identities.len());
		let mut enabled_flags = ColumnData::bool_with_capacity(identities.len());
		let mut identity_ids = ColumnData::identity_id_with_capacity(identities.len());

		for u in identities {
			ids.push(u.id);
			names.push(u.name.as_str());
			enabled_flags.push(u.enabled);
			identity_ids.push(u.id);
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
			Column {
				name: Fragment::internal("enabled"),
				data: enabled_flags,
			},
			Column {
				name: Fragment::internal("identity"),
				data: identity_ids,
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
