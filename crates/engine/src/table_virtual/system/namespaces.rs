// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system namespace information
pub struct Namespaces {
	definition: Arc<TableVirtualDef>,
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

impl<'a> TableVirtual<'a> for Namespaces {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
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
				name: Fragment::owned_internal("id"),
				data: ColumnData::uint8(namespace_ids),
			},
			Column {
				name: Fragment::owned_internal("name"),
				data: ColumnData::utf8(namespace_names),
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
