// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{SourceId, TableVirtualDef},
	retention::{CleanupMode, RetentionPolicy},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::{Fragment, Value};

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes source retention policy information
pub struct SourceRetentionPolicies {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl SourceRetentionPolicies {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_source_retention_policies_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<'a> TableVirtual<'a> for SourceRetentionPolicies {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let policies = CatalogStore::list_source_retention_policies(txn)?;

		let mut source_ids = ColumnData::uint8_with_capacity(policies.len());
		let mut source_types = ColumnData::utf8_with_capacity(policies.len());
		let mut policy_types = ColumnData::utf8_with_capacity(policies.len());
		let mut cleanup_modes = ColumnData::utf8_with_capacity(policies.len());
		let mut values = ColumnData::uint8_with_capacity(policies.len());

		for entry in policies {
			// Extract source ID and type
			let (source_id, source_type) = match entry.source {
				SourceId::Table(id) => (id.0, "table"),
				SourceId::View(id) => (id.0, "view"),
				SourceId::TableVirtual(id) => (id.0, "table_virtual"),
				SourceId::RingBuffer(id) => (id.0, "ring_buffer"),
			};

			source_ids.push(source_id);
			source_types.push(source_type);

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
				name: Fragment::owned_internal("source_id"),
				data: source_ids,
			},
			Column {
				name: Fragment::owned_internal("source_type"),
				data: source_types,
			},
			Column {
				name: Fragment::owned_internal("policy_type"),
				data: policy_types,
			},
			Column {
				name: Fragment::owned_internal("cleanup_mode"),
				data: cleanup_modes,
			},
			Column {
				name: Fragment::owned_internal("value"),
				data: values,
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
