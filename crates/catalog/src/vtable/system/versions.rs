// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	util::ioc::IocContainer,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemVersions {
	pub(crate) vtable: Arc<VTable>,
	ioc: IocContainer,
	exhausted: bool,
}

impl SystemVersions {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			vtable: SystemCatalog::get_system_versions_table().clone(),
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

		let mut names_to_insert = ColumnBuffer::utf8_with_capacity(versions.len());

		let mut versions_to_insert = ColumnBuffer::utf8_with_capacity(versions.len());

		let mut descriptions_to_insert = ColumnBuffer::utf8_with_capacity(versions.len());

		let mut types_to_insert = ColumnBuffer::utf8_with_capacity(versions.len());

		for version in versions {
			names_to_insert.push(version.name.as_str());
			versions_to_insert.push(version.version.as_str());
			descriptions_to_insert.push(version.description.as_str());
			types_to_insert.push(version.r#type.to_string().as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("name"), names_to_insert),
			ColumnWithName::new(Fragment::internal("version"), versions_to_insert),
			ColumnWithName::new(Fragment::internal("description"), descriptions_to_insert),
			ColumnWithName::new(Fragment::internal("type"), types_to_insert),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
