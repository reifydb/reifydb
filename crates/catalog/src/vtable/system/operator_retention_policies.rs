// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::VTableDef,
	retention::{CleanupMode, RetentionPolicy},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{Fragment, Value};

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes operator retention policy information
pub struct OperatorRetentionPolicies {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl OperatorRetentionPolicies {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_operator_retention_policies_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: IntoStandardTransaction> VTable<T> for OperatorRetentionPolicies {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let policies = CatalogStore::list_operator_retention_policies(txn).await?;

		let mut operator_ids = ColumnData::uint8_with_capacity(policies.len());
		let mut policy_types = ColumnData::utf8_with_capacity(policies.len());
		let mut cleanup_modes = ColumnData::utf8_with_capacity(policies.len());
		let mut values = ColumnData::uint8_with_capacity(policies.len());

		for entry in policies {
			operator_ids.push(entry.operator.0);

			// Encode policy
			match entry.policy {
				RetentionPolicy::KeepForever => {
					policy_types.push("keep_forever");
					cleanup_modes.push_value(Value::Undefined);
					values.push_value(Value::Undefined);
				}
				RetentionPolicy::KeepVersions {
					count,
					cleanup_mode,
				} => {
					policy_types.push("keep_versions");
					cleanup_modes.push(match cleanup_mode {
						CleanupMode::Delete => "delete",
						CleanupMode::Drop => "drop",
					});
					values.push(count);
				}
			}
		}

		let columns = vec![
			Column {
				name: Fragment::internal("operator_id"),
				data: operator_ids,
			},
			Column {
				name: Fragment::internal("policy_type"),
				data: policy_types,
			},
			Column {
				name: Fragment::internal("cleanup_mode"),
				data: cleanup_modes,
			},
			Column {
				name: Fragment::internal("value"),
				data: values,
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
