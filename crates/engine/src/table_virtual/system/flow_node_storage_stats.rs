// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::{ObjectId, StorageTracker, Tier};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes storage statistics for flow nodes
pub struct FlowNodeStorageStats {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	stats_tracker: StorageTracker,
}

impl FlowNodeStorageStats {
	pub fn new(stats_tracker: StorageTracker) -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_node_storage_stats_table_def().clone(),
			exhausted: false,
			stats_tracker,
		}
	}
}

fn tier_to_str(tier: Tier) -> &'static str {
	match tier {
		Tier::Hot => "hot",
		Tier::Warm => "warm",
		Tier::Cold => "cold",
	}
}

impl<'a> TableVirtual<'a> for FlowNodeStorageStats {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		// Collect all flow node stats across all tiers
		let mut rows: Vec<(u64, u64, Tier, u64, u64, u64, u64, u64, u64, u64, u64, u64)> = Vec::new();

		for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
			for (obj_id, stats) in self.stats_tracker.objects_by_tier(tier) {
				// Filter for flow nodes only
				if let ObjectId::FlowNode(flow_node_id) = obj_id {
					// Look up flow_id from catalog
					let flow_id = match CatalogStore::find_flow_node(txn, flow_node_id)? {
						Some(node_def) => node_def.flow.0,
						None => 0,
					};

					rows.push((
						flow_node_id.0,
						flow_id,
						tier,
						stats.current_key_bytes,
						stats.current_value_bytes,
						stats.current_bytes(),
						stats.current_entry_count,
						stats.historical_key_bytes,
						stats.historical_value_bytes,
						stats.historical_bytes(),
						stats.historical_entry_count,
						stats.total_bytes(),
					));
				}
			}
		}

		let capacity = rows.len();
		let mut ids = ColumnData::uint8_with_capacity(capacity);
		let mut flow_ids = ColumnData::uint8_with_capacity(capacity);
		let mut tiers = ColumnData::utf8_with_capacity(capacity);
		let mut current_key_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut current_value_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut current_total_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut current_entry_counts = ColumnData::uint8_with_capacity(capacity);
		let mut historical_key_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut historical_value_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut historical_total_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut historical_entry_counts = ColumnData::uint8_with_capacity(capacity);
		let mut total_bytes = ColumnData::uint8_with_capacity(capacity);

		for row in rows {
			ids.push(row.0);
			flow_ids.push(row.1);
			tiers.push(tier_to_str(row.2));
			current_key_bytes.push(row.3);
			current_value_bytes.push(row.4);
			current_total_bytes.push(row.5);
			current_entry_counts.push(row.6);
			historical_key_bytes.push(row.7);
			historical_value_bytes.push(row.8);
			historical_total_bytes.push(row.9);
			historical_entry_counts.push(row.10);
			total_bytes.push(row.11);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::owned_internal("flow_id"),
				data: flow_ids,
			},
			Column {
				name: Fragment::owned_internal("tier"),
				data: tiers,
			},
			Column {
				name: Fragment::owned_internal("current_key_bytes"),
				data: current_key_bytes,
			},
			Column {
				name: Fragment::owned_internal("current_value_bytes"),
				data: current_value_bytes,
			},
			Column {
				name: Fragment::owned_internal("current_total_bytes"),
				data: current_total_bytes,
			},
			Column {
				name: Fragment::owned_internal("current_entry_count"),
				data: current_entry_counts,
			},
			Column {
				name: Fragment::owned_internal("historical_key_bytes"),
				data: historical_key_bytes,
			},
			Column {
				name: Fragment::owned_internal("historical_value_bytes"),
				data: historical_value_bytes,
			},
			Column {
				name: Fragment::owned_internal("historical_total_bytes"),
				data: historical_total_bytes,
			},
			Column {
				name: Fragment::owned_internal("historical_entry_count"),
				data: historical_entry_counts,
			},
			Column {
				name: Fragment::owned_internal("total_bytes"),
				data: total_bytes,
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
