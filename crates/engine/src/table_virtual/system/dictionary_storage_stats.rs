// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{SourceId, TableVirtualDef},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::{ObjectId, StorageTracker, Tier};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes storage statistics for dictionaries
pub struct DictionaryStorageStats {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	stats_tracker: StorageTracker,
}

impl DictionaryStorageStats {
	pub fn new(stats_tracker: StorageTracker) -> Self {
		Self {
			definition: SystemCatalog::get_system_dictionary_storage_stats_table_def().clone(),
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

#[async_trait]
impl TableVirtual for DictionaryStorageStats {
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

		// Collect all dictionary stats across all tiers
		#[allow(clippy::type_complexity)]
		let mut rows: Vec<(u64, u64, Tier, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64)> = Vec::new();

		for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
			for (obj_id, stats) in self.stats_tracker.objects_by_tier(tier) {
				// Filter for dictionary sources only
				if let ObjectId::Source(SourceId::Dictionary(dictionary_id)) = obj_id {
					// Look up namespace_id from catalog
					let namespace_id =
						match CatalogStore::find_dictionary(txn, dictionary_id).await? {
							Some(dictionary_def) => dictionary_def.namespace.0,
							None => 0,
						};

					rows.push((
						dictionary_id.0,
						namespace_id,
						tier,
						stats.current_key_bytes,
						stats.current_value_bytes,
						stats.current_bytes(),
						stats.current_count,
						stats.historical_key_bytes,
						stats.historical_value_bytes,
						stats.historical_bytes(),
						stats.historical_count,
						stats.total_bytes(),
						stats.cdc_key_bytes,
						stats.cdc_value_bytes,
						stats.cdc_total_bytes(),
						stats.cdc_count,
					));
				}
			}
		}

		let capacity = rows.len();
		let mut ids = ColumnData::uint8_with_capacity(capacity);
		let mut namespace_ids = ColumnData::uint8_with_capacity(capacity);
		let mut tiers = ColumnData::utf8_with_capacity(capacity);
		let mut current_key_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut current_value_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut current_total_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut current_counts = ColumnData::uint8_with_capacity(capacity);
		let mut historical_key_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut historical_value_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut historical_total_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut historical_counts = ColumnData::uint8_with_capacity(capacity);
		let mut total_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut cdc_key_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut cdc_value_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut cdc_total_bytes = ColumnData::uint8_with_capacity(capacity);
		let mut cdc_counts = ColumnData::uint8_with_capacity(capacity);

		for row in rows {
			ids.push(row.0);
			namespace_ids.push(row.1);
			tiers.push(tier_to_str(row.2));
			current_key_bytes.push(row.3);
			current_value_bytes.push(row.4);
			current_total_bytes.push(row.5);
			current_counts.push(row.6);
			historical_key_bytes.push(row.7);
			historical_value_bytes.push(row.8);
			historical_total_bytes.push(row.9);
			historical_counts.push(row.10);
			total_bytes.push(row.11);
			cdc_key_bytes.push(row.12);
			cdc_value_bytes.push(row.13);
			cdc_total_bytes.push(row.14);
			cdc_counts.push(row.15);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespace_ids,
			},
			Column {
				name: Fragment::internal("tier"),
				data: tiers,
			},
			Column {
				name: Fragment::internal("current_key_bytes"),
				data: current_key_bytes,
			},
			Column {
				name: Fragment::internal("current_value_bytes"),
				data: current_value_bytes,
			},
			Column {
				name: Fragment::internal("current_total_bytes"),
				data: current_total_bytes,
			},
			Column {
				name: Fragment::internal("current_count"),
				data: current_counts,
			},
			Column {
				name: Fragment::internal("historical_key_bytes"),
				data: historical_key_bytes,
			},
			Column {
				name: Fragment::internal("historical_value_bytes"),
				data: historical_value_bytes,
			},
			Column {
				name: Fragment::internal("historical_total_bytes"),
				data: historical_total_bytes,
			},
			Column {
				name: Fragment::internal("historical_count"),
				data: historical_counts,
			},
			Column {
				name: Fragment::internal("total_bytes"),
				data: total_bytes,
			},
			Column {
				name: Fragment::internal("cdc_key_bytes"),
				data: cdc_key_bytes,
			},
			Column {
				name: Fragment::internal("cdc_value_bytes"),
				data: cdc_value_bytes,
			},
			Column {
				name: Fragment::internal("cdc_total_bytes"),
				data: cdc_total_bytes,
			},
			Column {
				name: Fragment::internal("cdc_count"),
				data: cdc_counts,
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
