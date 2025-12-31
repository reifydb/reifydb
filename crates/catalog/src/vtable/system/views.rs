// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{Batch, QueryTransaction, VTableDef},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::{Fragment, Value};

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{VTable, VTableContext},
};

/// Virtual table that exposes system view information
pub struct Views {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Views {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_views_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: QueryTransaction> VTable<T> for Views {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let views = CatalogStore::list_views_all(txn).await?;

		let mut ids = ColumnData::uint8_with_capacity(views.len());
		let mut namespaces = ColumnData::uint8_with_capacity(views.len());
		let mut names = ColumnData::utf8_with_capacity(views.len());
		let mut primary_keys = ColumnData::uint4_with_capacity(views.len());

		for view in views {
			ids.push(view.id.0);
			namespaces.push(view.namespace.0);
			names.push(view.name.as_str());
			primary_keys.push_value(
				view.primary_key.map(|pk| pk.id.0).map(Value::Uint8).unwrap_or(Value::Undefined),
			);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespaces,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("primary_key_id"),
				data: primary_keys,
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
