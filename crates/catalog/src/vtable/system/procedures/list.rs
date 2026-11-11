// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::Procedure, vtable::VTable},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemProcedures {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemProcedures {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_procedures_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemProcedures {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut procs: Vec<Procedure> =
			CatalogStore::list_procedures_all(txn)?.into_iter().filter(Procedure::is_persistent).collect();
		procs.extend(self
			.catalog
			.cache
			.procedures
			.iter()
			.filter_map(|entry| entry.value().get_latest())
			.filter(|p| !p.is_persistent()));

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, procs.len());
		let mut namespace_ids = ColumnBuilder::with_capacity(ValueType::Uint8, procs.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());
		let mut kinds = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());

		for p in &procs {
			ids.push(*p.id());
			namespace_ids.push(p.namespace().0);
			names.push(p.name());
			kinds.push(p.kind().as_str());
		}

		let columns = vec![
			ids.finish("id"),
			namespace_ids.finish("namespace_id"),
			names.finish("name"),
			kinds.finish("kind"),
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
