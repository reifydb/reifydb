// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{VTableDef, version::SystemVersion},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Fragment;

use crate::{
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system version information
pub struct Versions {
	pub(crate) definition: Arc<VTableDef>,
	/// Versions data is stored here since it's static and set once at system initialization
	versions: Vec<SystemVersion>,
	exhausted: bool,
}

impl Versions {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_versions_table_def().clone(),
			versions: Vec::new(),
			exhausted: false,
		}
	}

	/// Create a new Versions table with the provided version data
	pub fn with_versions(versions: Vec<SystemVersion>) -> Self {
		Self {
			definition: SystemCatalog::get_system_versions_table_def().clone(),
			versions,
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: IntoStandardTransaction> VTable<T> for Versions {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Versions are stored in the struct since they're static
		let versions = &self.versions;

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
