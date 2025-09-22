// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::system::SystemCatalog;
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnComputed, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system version information
pub struct Versions<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Versions<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_versions_table_def().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for Versions<T> {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a, T>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> Result<Option<Batch<'a>>> {
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
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("name"),
				data: names_to_insert,
			}),
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("version"),
				data: versions_to_insert,
			}),
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("description"),
				data: descriptions_to_insert,
			}),
			Column::Computed(ColumnComputed {
				name: Fragment::owned_internal("type"),
				data: types_to_insert,
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
