// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{primitive::PrimitiveId, vtable::VTableDef},
	retention::{CleanupMode, RetentionPolicy},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes primitive retention policy information
pub struct PrimitiveRetentionPolicies {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl PrimitiveRetentionPolicies {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_primitive_retention_policies_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for PrimitiveRetentionPolicies {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let policies = CatalogStore::list_primitive_retention_policies(txn)?;

		let mut primitive_ids = ColumnData::uint8_with_capacity(policies.len());
		let mut primitive_types = ColumnData::utf8_with_capacity(policies.len());
		let mut policy_types = ColumnData::utf8_with_capacity(policies.len());
		let mut cleanup_modes = ColumnData::utf8_with_capacity(policies.len());
		let mut values = ColumnData::uint8_with_capacity(policies.len());

		for entry in policies {
			// Extract primitive ID and type
			let (primitive_id, primitive_type) = match entry.primitive {
				PrimitiveId::Table(id) => (id.0, "table"),
				PrimitiveId::View(id) => (id.0, "view"),
				PrimitiveId::TableVirtual(id) => (id.0, "vtable"),
				PrimitiveId::RingBuffer(id) => (id.0, "ringbuffer"),
				PrimitiveId::Flow(id) => (id.0, "flow"),
				PrimitiveId::Dictionary(id) => (id.0, "dictionary"),
				PrimitiveId::Series(id) => (id.0, "series"),
			};

			primitive_ids.push(primitive_id);
			primitive_types.push(primitive_type);

			// Encode policy
			match entry.policy {
				RetentionPolicy::KeepForever => {
					policy_types.push("keep_forever");
					cleanup_modes.push_value(Value::none());
					values.push_value(Value::none());
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
				name: Fragment::internal("primitive_id"),
				data: primitive_ids,
			},
			Column {
				name: Fragment::internal("primitive_type"),
				data: primitive_types,
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
