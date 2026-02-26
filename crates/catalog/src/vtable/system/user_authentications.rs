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

/// Virtual table that exposes system user authentication information
pub struct UserAuthentications {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl UserAuthentications {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_user_authentications_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for UserAuthentications {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let auths = CatalogStore::list_all_user_authentications(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(auths.len());
		let mut user_ids = ColumnData::uint8_with_capacity(auths.len());
		let mut methods = ColumnData::utf8_with_capacity(auths.len());

		for a in auths {
			ids.push(a.id);
			user_ids.push(a.user_id);
			methods.push(a.method.as_str());
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("user_id"),
				data: user_ids,
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
