// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::{PrimitiveId, VTableDef};
use reifydb_core::value::column::{Column, ColumnData, Columns};
use reifydb_metric::{Id, MetricReader, Tier};
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes storage statistics for views
pub struct ViewStorageStats {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
	stats_reader: MetricReader<TransactionStore>,
}

impl ViewStorageStats {
	pub fn new(stats_reader: MetricReader<TransactionStore>) -> Self {
		Self {
			definition: SystemCatalog::get_system_view_storage_stats_table_def().clone(),
			exhausted: false,
			stats_reader,
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

impl<T: IntoStandardTransaction> VTable<T> for ViewStorageStats {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Collect all view stats across all tiers
		#[allow(clippy::type_complexity)]
		let mut rows: Vec<(u64, u64, Tier, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64)> = Vec::new();

		for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
			let tier_stats = self.stats_reader.scan_tier(tier).unwrap_or_default();
			for (obj_id, stats) in tier_stats {
				// Filter for view sources only
				if let Id::Source(PrimitiveId::View(view_id)) = obj_id {
					// Look up namespace_id from catalog
					let namespace_id = match CatalogStore::find_view(txn, view_id)? {
						Some(view_def) => view_def.namespace.0,
						None => 0,
					};

					rows.push((
						view_id.0,
						namespace_id,
						tier,
						stats.storage.current_key_bytes,
						stats.storage.current_value_bytes,
						stats.current_bytes(),
						stats.storage.current_count,
						stats.storage.historical_key_bytes,
						stats.storage.historical_value_bytes,
						stats.historical_bytes(),
						stats.storage.historical_count,
						stats.total_bytes(),
						stats.cdc.key_bytes,
						stats.cdc.value_bytes,
						stats.cdc_total_bytes(),
						stats.cdc.entry_count,
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
