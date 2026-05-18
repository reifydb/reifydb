// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::{catalog::vtable::VTable, store::Tier},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_metric::storage::{metric::MetricReader, multi::MultiStorageStats};
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::StatsPrimitive;
use crate::{
	Result,
	vtable::{BaseVTable, Batch, VTableContext},
};

type StorageRow = (u64, u64, Tier, u64, u64, u64, u64, u64, u64, u64, u64, u64);

pub struct SystemMetricsStorage {
	pub(crate) vtable: Arc<VTable>,
	primitive: StatsPrimitive,
	stats_reader: MetricReader<SingleStore>,
	exhausted: bool,
}

impl SystemMetricsStorage {
	pub fn new(vtable: Arc<VTable>, primitive: StatsPrimitive, stats_reader: MetricReader<SingleStore>) -> Self {
		Self {
			vtable,
			primitive,
			stats_reader,
			exhausted: false,
		}
	}
}

fn tier_to_str(tier: Tier) -> &'static str {
	match tier {
		Tier::Buffer => "buffer",
		Tier::Persistent => "persistent",
	}
}

impl BaseVTable for SystemMetricsStorage {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let rows = if self.primitive == StatsPrimitive::Flow {
			self.collect_flow_rows(txn)?
		} else {
			self.collect_simple_rows(txn)?
		};

		self.exhausted = true;
		Ok(Some(Batch {
			columns: build_columns(rows),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}

impl SystemMetricsStorage {
	fn collect_simple_rows(&self, txn: &mut Transaction<'_>) -> Result<Vec<StorageRow>> {
		let mut rows: Vec<StorageRow> = Vec::new();
		for tier in [Tier::Buffer, Tier::Persistent] {
			let tier_stats = self.stats_reader.scan_tier(tier).unwrap_or_default();
			for (metric_id, stats) in tier_stats {
				if let Some(row) = self.primitive.match_metric_id(txn, metric_id)? {
					rows.push((
						row.id,
						row.namespace_id,
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
					));
				}
			}
		}
		Ok(rows)
	}

	fn collect_flow_rows(&self, txn: &mut Transaction<'_>) -> Result<Vec<StorageRow>> {
		let mut aggregated: HashMap<(u64, u64, Tier), MultiStorageStats> = HashMap::new();

		for tier in [Tier::Buffer, Tier::Persistent] {
			let tier_stats = self.stats_reader.scan_tier(tier).unwrap_or_default();
			for (metric_id, stats) in tier_stats {
				if let Some(row) = self.primitive.match_metric_id(txn, metric_id)? {
					let entry = aggregated.entry((row.id, row.namespace_id, tier)).or_default();
					*entry += stats.storage;
				}
			}
		}

		Ok(aggregated
			.into_iter()
			.map(|((flow_id, namespace_id, tier), storage)| {
				(
					flow_id,
					namespace_id,
					tier,
					storage.current_key_bytes,
					storage.current_value_bytes,
					storage.current_bytes(),
					storage.current_count,
					storage.historical_key_bytes,
					storage.historical_value_bytes,
					storage.historical_bytes(),
					storage.historical_count,
					storage.total_bytes(),
				)
			})
			.collect())
	}
}

fn build_columns(rows: Vec<StorageRow>) -> Columns {
	let capacity = rows.len();
	let mut ids = ColumnBuffer::uint8_with_capacity(capacity);
	let mut namespace_ids = ColumnBuffer::uint8_with_capacity(capacity);
	let mut tiers = ColumnBuffer::utf8_with_capacity(capacity);
	let mut current_key_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut current_value_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut current_total_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut current_counts = ColumnBuffer::uint8_with_capacity(capacity);
	let mut historical_key_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut historical_value_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut historical_total_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut historical_counts = ColumnBuffer::uint8_with_capacity(capacity);
	let mut total_bytes = ColumnBuffer::uint8_with_capacity(capacity);

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
	}

	Columns::new(vec![
		ColumnWithName::new(Fragment::internal("id"), ids),
		ColumnWithName::new(Fragment::internal("namespace_id"), namespace_ids),
		ColumnWithName::new(Fragment::internal("tier"), tiers),
		ColumnWithName::new(Fragment::internal("current_key_bytes"), current_key_bytes),
		ColumnWithName::new(Fragment::internal("current_value_bytes"), current_value_bytes),
		ColumnWithName::new(Fragment::internal("current_total_bytes"), current_total_bytes),
		ColumnWithName::new(Fragment::internal("current_count"), current_counts),
		ColumnWithName::new(Fragment::internal("historical_key_bytes"), historical_key_bytes),
		ColumnWithName::new(Fragment::internal("historical_value_bytes"), historical_value_bytes),
		ColumnWithName::new(Fragment::internal("historical_total_bytes"), historical_total_bytes),
		ColumnWithName::new(Fragment::internal("historical_count"), historical_counts),
		ColumnWithName::new(Fragment::internal("total_bytes"), total_bytes),
	])
}
