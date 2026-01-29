// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	util::ioc::IocContainer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system version information
pub struct Versions {
	pub(crate) definition: Arc<VTableDef>,
	ioc: IocContainer,
	exhausted: bool,
}

impl Versions {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			definition: SystemCatalog::get_system_versions_table_def().clone(),
			ioc,
			exhausted: false,
		}
	}
}

impl<T: AsTransaction> VTable<T> for Versions {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let versions = match self.ioc.resolve::<SystemCatalog>() {
			Ok(catalog) => catalog.get_system_versions().to_vec(),
			Err(_) => vec![],
		};

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
