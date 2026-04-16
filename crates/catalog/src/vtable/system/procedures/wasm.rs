// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::Procedure, vtable::VTable},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes WASM procedures loaded from compiled modules.
pub struct SystemProceduresWasm {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemProceduresWasm {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_procedures_wasm_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemProceduresWasm {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut id_col = ColumnData::uint8_with_capacity(0);
		let mut ns_col = ColumnData::uint8_with_capacity(0);
		let mut name_col = ColumnData::utf8_with_capacity(0);
		let mut native_col = ColumnData::utf8_with_capacity(0);
		let mut module_col = ColumnData::uint8_with_capacity(0);

		for entry in self.catalog.materialized.procedures.iter() {
			if let Some(Procedure::Wasm {
				id,
				namespace,
				name,
				native_name,
				module_id,
				..
			}) = entry.value().get_latest()
			{
				id_col.push(*id);
				ns_col.push(namespace.0);
				name_col.push(name.as_str());
				native_col.push(native_name.as_str());
				module_col.push(module_id.0);
			}
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: id_col,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: ns_col,
			},
			Column {
				name: Fragment::internal("name"),
				data: name_col,
			},
			Column {
				name: Fragment::internal("native_name"),
				data: native_col,
			},
			Column {
				name: Fragment::internal("module_id"),
				data: module_col,
			},
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
