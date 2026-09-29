// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	util::ioc::IocContainer,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

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

		let versions = match self.ioc.try_resolve::<SystemCatalog>() {
			Some(catalog) => catalog.get_system_versions().to_vec(),
			None => vec![],
		};

		let mut names_to_insert = ColumnBuilder::with_capacity(ValueType::Utf8, versions.len());

		let mut versions_to_insert = ColumnBuilder::with_capacity(ValueType::Utf8, versions.len());

		let mut descriptions_to_insert = ColumnBuilder::with_capacity(ValueType::Utf8, versions.len());

		let mut types_to_insert = ColumnBuilder::with_capacity(ValueType::Utf8, versions.len());

		for version in versions {
			names_to_insert.push(version.name.as_str());
			versions_to_insert.push(version.version.as_str());
			descriptions_to_insert.push(version.description.as_str());
			types_to_insert.push(version.r#type.to_string().as_str());
		}

		let columns = vec![
			names_to_insert.finish("name"),
			versions_to_insert.finish("version"),
			descriptions_to_insert.finish("description"),
			types_to_insert.finish("type"),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
