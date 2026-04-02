// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{shape::ShapeId, vtable::VTable},
	retention::{CleanupMode, RetentionPolicy},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes object retention policy information
pub struct SystemShapeRetentionPolicies {
	pub(crate) definition: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemShapeRetentionPolicies {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemShapeRetentionPolicies {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_shape_retention_policies_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemShapeRetentionPolicies {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let policies = CatalogStore::list_shape_retention_policies(txn)?;

		let mut primitive_ids = ColumnData::uint8_with_capacity(policies.len());
		let mut shape_types = ColumnData::utf8_with_capacity(policies.len());
		let mut policy_types = ColumnData::utf8_with_capacity(policies.len());
		let mut cleanup_modes = ColumnData::utf8_with_capacity(policies.len());
		let mut values = ColumnData::uint8_with_capacity(policies.len());

		for entry in policies {
			// Extract object ID and type
			let (object_id, shape_type) = match entry.shape {
				ShapeId::Table(id) => (id.0, "table"),
				ShapeId::View(id) => (id.0, "view"),
				ShapeId::TableVirtual(id) => (id.0, "vtable"),
				ShapeId::RingBuffer(id) => (id.0, "ringbuffer"),
				ShapeId::Dictionary(id) => (id.0, "dictionary"),
				ShapeId::Series(id) => (id.0, "series"),
			};

			primitive_ids.push(object_id);
			shape_types.push(shape_type);

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
				name: Fragment::internal("object_id"),
				data: primitive_ids,
			},
			Column {
				name: Fragment::internal("shape_type"),
				data: shape_types,
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

	fn definition(&self) -> &VTable {
		&self.definition
	}
}
