// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system policy operation information
pub struct PolicyOperations {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl PolicyOperations {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_policy_operations_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for PolicyOperations {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let ops = CatalogStore::list_all_policy_operations(txn)?;

		let mut policy_ids = ColumnData::uint8_with_capacity(ops.len());
		let mut operations = ColumnData::utf8_with_capacity(ops.len());
		let mut body_sources = ColumnData::utf8_with_capacity(ops.len());

		for op in ops {
			policy_ids.push(op.policy_id);
			operations.push(op.operation.as_str());
			body_sources.push(op.body_source.as_str());
		}

		let columns = vec![
			Column {
				name: Fragment::internal("policy_id"),
				data: policy_ids,
			},
			Column {
				name: Fragment::internal("operation"),
				data: operations,
			},
			Column {
				name: Fragment::internal("body_source"),
				data: body_sources,
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
