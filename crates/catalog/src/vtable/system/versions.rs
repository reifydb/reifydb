// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	util::ioc::IocContainer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system version information
pub struct SystemVersions {
	pub(crate) definition: Arc<VTable>,
	ioc: IocContainer,
	exhausted: bool,
}

impl SystemVersions {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			definition: SystemCatalog::get_system_versions_table().clone(),
			ioc,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemVersions {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
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

	fn definition(&self) -> &VTable {
		&self.definition
	}
}
