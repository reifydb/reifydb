// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::ProcedureTrigger, vtable::VTableDef},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes procedures with trigger = Call
pub struct Procedures {
	pub(crate) definition: Arc<VTableDef>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl Procedures {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_procedures_table_def().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl VTable for Procedures {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut names = Vec::new();

		for entry in self.catalog.materialized.procedures.iter() {
			if let Some(proc_def) = entry.value().get_latest() {
				if matches!(proc_def.trigger, ProcedureTrigger::Call) {
					ids.push(proc_def.id.0);
					namespace_ids.push(proc_def.namespace.0);
					names.push(proc_def.name.clone());
				}
			}
		}

		let len = ids.len();
		let mut id_col = ColumnData::uint8_with_capacity(len);
		let mut ns_col = ColumnData::uint8_with_capacity(len);
		let mut name_col = ColumnData::utf8_with_capacity(len);

		for id in &ids {
			id_col.push(*id);
		}
		for ns in &namespace_ids {
			ns_col.push(*ns);
		}
		for name in &names {
			name_col.push(name.as_str());
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
