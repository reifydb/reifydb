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

/// Virtual table that exposes Native (built-in Rust) procedures from the materialized cache.
pub struct SystemProceduresNative {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemProceduresNative {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_procedures_native_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemProceduresNative {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut names = Vec::new();
		let mut native_names = Vec::new();

		for entry in self.catalog.materialized.procedures.iter() {
			if let Some(Procedure::Native {
				id,
				namespace,
				name,
				native_name,
				..
			}) = entry.value().get_latest()
			{
				ids.push(*id);
				namespace_ids.push(namespace.0);
				names.push(name.clone());
				native_names.push(native_name.clone());
			}
		}

		let len = ids.len();
		let mut id_col = ColumnBuffer::uint8_with_capacity(len);
		let mut ns_col = ColumnBuffer::uint8_with_capacity(len);
		let mut name_col = ColumnBuffer::utf8_with_capacity(len);
		let mut native_col = ColumnBuffer::utf8_with_capacity(len);

		for v in &ids {
			id_col.push(*v);
		}
		for v in &namespace_ids {
			ns_col.push(*v);
		}
		for v in &names {
			name_col.push(v.as_str());
		}
		for v in &native_names {
			native_col.push(v.as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), id_col),
			ColumnWithName::new(Fragment::internal("namespace_id"), ns_col),
			ColumnWithName::new(Fragment::internal("name"), name_col),
			ColumnWithName::new(Fragment::internal("native_name"), native_col),
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
