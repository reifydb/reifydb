// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::VTableDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system namespace information
pub struct Namespaces {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Namespaces {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_namespaces_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: IntoStandardTransaction> VTable<T> for Namespaces {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut namespace_ids = Vec::new();
		let mut namespace_names = Vec::new();

		let namespaces = CatalogStore::list_namespaces_all(txn)?;
		for namespace in namespaces {
			namespace_ids.push(namespace.id.0);
			namespace_names.push(namespace.name);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(namespace_ids),
			},
			Column {
				name: Fragment::internal("name"),
				data: ColumnData::utf8(namespace_names),
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
