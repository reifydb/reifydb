// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{Batch, QueryTransaction, VTableDef},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	system::SystemCatalog,
	transaction::MaterializedCatalogTransaction,
	vtable::{VTable, VTableContext},
};

/// Virtual table that exposes system version information
pub struct Versions {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Versions {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_versions_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: QueryTransaction + MaterializedCatalogTransaction> VTable<T> for Versions {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Get versions from SystemCatalog via MaterializedCatalog
		let versions = txn.catalog().system_catalog().map(|sc| sc.get_system_versions()).unwrap_or(&[]);

		let mut names_to_insert = ColumnData::utf8_with_capacity(versions.len());

		let mut versions_to_insert = ColumnData::utf8_with_capacity(versions.len());

		let mut descriptions_to_insert = ColumnData::utf8_with_capacity(versions.len());

		let mut types_to_insert = ColumnData::utf8_with_capacity(versions.len());

		for version in versions {
			names_to_insert.push(version.name.as_str());
			versions_to_insert.push(version.version.as_str());
			descriptions_to_insert.push(version.description.as_str());
			types_to_insert.push(version.r#type.to_string().as_str());
		}

		let columns = vec![
			Column {
				name: Fragment::internal("name"),
				data: names_to_insert,
			},
			Column {
				name: Fragment::internal("version"),
				data: versions_to_insert,
			},
			Column {
				name: Fragment::internal("description"),
				data: descriptions_to_insert,
			},
			Column {
				name: Fragment::internal("type"),
				data: types_to_insert,
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
