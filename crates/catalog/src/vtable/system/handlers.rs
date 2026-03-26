// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		procedure::{Procedure, ProcedureTrigger},
		vtable::VTable,
	},
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

/// Virtual table that exposes procedures with trigger = Event (event handlers)
pub struct SystemHandlers {
	pub(crate) definition: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemHandlers {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_handlers_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemHandlers {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut names = Vec::new();
		let mut sumtype_ids = Vec::new();
		let mut variant_tags = Vec::new();

		let mut collect = |proc_def: &Procedure| {
			if let ProcedureTrigger::Event {
				sumtype_id,
				variant_tag,
			} = &proc_def.trigger
			{
				if !ids.contains(&proc_def.id.0) {
					ids.push(proc_def.id.0);
					namespace_ids.push(proc_def.namespace.0);
					names.push(proc_def.name.clone());
					sumtype_ids.push(sumtype_id.0);
					variant_tags.push(*variant_tag);
				}
			}
		};

		for entry in self.catalog.materialized.procedures.iter() {
			if let Some(ref proc_def) = entry.value().get_latest() {
				collect(proc_def);
			}
		}

		if let Transaction::Test(t) = txn {
			for change in &t.inner.changes.procedure {
				if let Some(proc_def) = &change.post {
					collect(proc_def);
				}
			}
		}

		let len = ids.len();
		let mut id_col = ColumnData::uint8_with_capacity(len);
		let mut ns_col = ColumnData::uint8_with_capacity(len);
		let mut name_col = ColumnData::utf8_with_capacity(len);
		let mut sumtype_col = ColumnData::uint8_with_capacity(len);
		let mut tag_col = ColumnData::uint1_with_capacity(len);

		for id in &ids {
			id_col.push(*id);
		}
		for ns in &namespace_ids {
			ns_col.push(*ns);
		}
		for name in &names {
			name_col.push(name.as_str());
		}
		for st in &sumtype_ids {
			sumtype_col.push(*st);
		}
		for tag in &variant_tags {
			tag_col.push(*tag);
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
				name: Fragment::internal("on_sumtype_id"),
				data: sumtype_col,
			},
			Column {
				name: Fragment::internal("on_variant_tag"),
				data: tag_col,
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
