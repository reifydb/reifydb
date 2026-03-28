// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::ProcedureTrigger, vtable::VTable},
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

/// Virtual table that exposes procedures with trigger = Call or NativeCall
pub struct SystemProcedures {
	pub(crate) definition: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemProcedures {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_procedures_table().clone(),
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

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut names = Vec::new();
		let mut is_tests = Vec::new();

		for entry in self.catalog.materialized.procedures.iter() {
			if let Some(proc_def) = entry.value().get_latest() {
				if matches!(
					proc_def.trigger,
					ProcedureTrigger::Call | ProcedureTrigger::NativeCall { .. }
				) {
					ids.push(proc_def.id.0);
					namespace_ids.push(proc_def.namespace.0);
					names.push(proc_def.name.clone());
					is_tests.push(proc_def.is_test);
				}
			}
		}

		let len = ids.len();
		let mut id_col = ColumnData::uint8_with_capacity(len);
		let mut ns_col = ColumnData::uint8_with_capacity(len);
		let mut name_col = ColumnData::utf8_with_capacity(len);
		let mut is_test_col = ColumnData::bool_with_capacity(len);

		for id in &ids {
			id_col.push(*id);
		}
		for ns in &namespace_ids {
			ns_col.push(*ns);
		}
		for name in &names {
			name_col.push(name.as_str());
		}
		for is_test in &is_tests {
			is_test_col.push(*is_test);
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
				name: Fragment::internal("is_test"),
				data: is_test_col,
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
