// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemPolicyOperations {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemPolicyOperations {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemPolicyOperations {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_policy_operations_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemPolicyOperations {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let ops = CatalogStore::list_all_policy_operations(txn)?;

		let mut policy_ids = ColumnBuffer::uint8_with_capacity(ops.len());
		let mut operations = ColumnBuffer::utf8_with_capacity(ops.len());
		let mut body_sources = ColumnBuffer::utf8_with_capacity(ops.len());

		for op in ops {
			policy_ids.push(op.policy_id);
			operations.push(op.operation.as_str());
			body_sources.push(op.body_source.as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("policy_id"), policy_ids),
			ColumnWithName::new(Fragment::internal("operation"), operations),
			ColumnWithName::new(Fragment::internal("body_source"), body_sources),
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
