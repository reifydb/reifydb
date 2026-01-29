// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system column policy information
pub struct ColumnPolicies {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl ColumnPolicies {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_column_policies_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<T: AsTransaction> VTable<T> for ColumnPolicies {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut policy_ids = Vec::new();
		let mut column_ids = Vec::new();
		let mut policy_types = Vec::new();
		let mut policy_values = Vec::new();

		let policies = CatalogStore::list_column_policies_all(txn)?;
		for policy in policies {
			policy_ids.push(policy.id.0);
			column_ids.push(policy.column.0);
			let (ty, val) = policy.policy.to_u8();
			policy_types.push(ty);
			policy_values.push(val);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(policy_ids),
			},
			Column {
				name: Fragment::internal("column_id"),
				data: ColumnData::uint8(column_ids),
			},
			Column {
				name: Fragment::internal("type"),
				data: ColumnData::uint1(policy_types),
			},
			Column {
				name: Fragment::internal("value"),
				data: ColumnData::uint1(policy_values),
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
