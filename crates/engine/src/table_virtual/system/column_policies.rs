// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system column policy information
pub struct ColumnPolicies {
	definition: Arc<TableVirtualDef>,
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

#[async_trait]
impl TableVirtual for ColumnPolicies {
	async fn initialize<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		_ctx: TableVirtualContext,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next<'a>(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut policy_ids = Vec::new();
		let mut column_ids = Vec::new();
		let mut policy_types = Vec::new();
		let mut policy_values = Vec::new();

		let policies = CatalogStore::list_column_policies_all(txn).await?;
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

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
