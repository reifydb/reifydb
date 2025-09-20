// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system namespace information
pub struct Namespaces<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Namespaces<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_namespaces_table_def().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for Namespaces<T> {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a, T>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> Result<Option<Batch<'a>>> {
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
			Column::ColumnQualified(ColumnQualified {
				name: Fragment::owned_internal("id"),
				data: ColumnData::uint8(namespace_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: Fragment::owned_internal("name"),
				data: ColumnData::utf8(namespace_names),
			}),
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
