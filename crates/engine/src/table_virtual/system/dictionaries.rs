// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
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

/// Virtual table that exposes system dictionary information
pub struct Dictionaries {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl Dictionaries {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_dictionaries_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl TableVirtual for Dictionaries {
	async fn initialize<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		_ctx: TableVirtualContext,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next<'a>(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut names = Vec::new();
		let mut value_types = Vec::new();
		let mut id_types = Vec::new();

		let dictionaries = CatalogStore::list_all_dictionaries(txn).await?;
		for dict in dictionaries {
			ids.push(dict.id.0);
			namespace_ids.push(dict.namespace.0);
			names.push(dict.name);
			value_types.push(dict.value_type.to_string());
			id_types.push(dict.id_type.to_string());
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(ids),
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: ColumnData::uint8(namespace_ids),
			},
			Column {
				name: Fragment::internal("name"),
				data: ColumnData::utf8(names),
			},
			Column {
				name: Fragment::internal("value_type"),
				data: ColumnData::utf8(value_types),
			},
			Column {
				name: Fragment::internal("id_type"),
				data: ColumnData::utf8(id_types),
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
