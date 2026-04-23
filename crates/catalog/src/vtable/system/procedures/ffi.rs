// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::Procedure, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes FFI procedures loaded from shared libraries.
pub struct SystemProceduresFfi {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemProceduresFfi {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_procedures_ffi_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemProceduresFfi {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut id_col = ColumnBuffer::uint8_with_capacity(0);
		let mut ns_col = ColumnBuffer::uint8_with_capacity(0);
		let mut name_col = ColumnBuffer::utf8_with_capacity(0);
		let mut native_col = ColumnBuffer::utf8_with_capacity(0);
		let mut library_col = ColumnBuffer::utf8_with_capacity(0);
		let mut entry_col = ColumnBuffer::utf8_with_capacity(0);

		for entry in self.catalog.materialized.procedures.iter() {
			if let Some(Procedure::Ffi {
				id,
				namespace,
				name,
				native_name,
				library_path,
				entry_symbol,
				..
			}) = entry.value().get_latest()
			{
				id_col.push(*id);
				ns_col.push(namespace.0);
				name_col.push(name.as_str());
				native_col.push(native_name.as_str());
				library_col.push(library_path.to_string_lossy().as_ref());
				entry_col.push(entry_symbol.as_str());
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), id_col),
			ColumnWithName::new(Fragment::internal("namespace_id"), ns_col),
			ColumnWithName::new(Fragment::internal("name"), name_col),
			ColumnWithName::new(Fragment::internal("native_name"), native_col),
			ColumnWithName::new(Fragment::internal("library_path"), library_col),
			ColumnWithName::new(Fragment::internal("entry_symbol"), entry_col),
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
