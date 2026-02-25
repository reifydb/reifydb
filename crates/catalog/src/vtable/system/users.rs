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

/// Virtual table that exposes system user information
pub struct Users {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Users {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_users_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for Users {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let users = CatalogStore::list_all_users(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(users.len());
		let mut names = ColumnData::utf8_with_capacity(users.len());
		let mut password_hashes = ColumnData::utf8_with_capacity(users.len());
		let mut enabled_flags = ColumnData::bool_with_capacity(users.len());

		for u in users {
			ids.push(u.id);
			names.push(u.name.as_str());
			password_hashes.push(u.password_hash.as_str());
			enabled_flags.push(u.enabled);
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
				name: Fragment::internal("password_hash"),
				data: password_hashes,
			},
			Column {
				name: Fragment::internal("enabled"),
				data: enabled_flags,
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
