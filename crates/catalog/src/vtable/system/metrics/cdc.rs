// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_metric::{
	MetricId,
	storage::{cdc::CdcStats, metric::MetricReader},
};
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::StatsPrimitive;
use crate::{
	Result,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Row: (id, namespace_id, key_bytes, value_bytes, total_bytes, count).
type CdcRow = (u64, u64, u64, u64, u64, u64);

/// Generic CDC-stats virtual table. One row per object id for the matched
/// `StatsPrimitive`. No tier dimension - CDC stats are not tiered.
pub struct SystemMetricsCdc {
	pub(crate) vtable: Arc<VTable>,
	primitive: StatsPrimitive,
	stats_reader: MetricReader<SingleStore>,
	exhausted: bool,
}

impl SystemMetricsCdc {
	pub fn new(vtable: Arc<VTable>, primitive: StatsPrimitive, stats_reader: MetricReader<SingleStore>) -> Self {
		Self {
			vtable,
			primitive,
			stats_reader,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemMetricsCdc {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let all = self.stats_reader.cdc_reader().scan_all().unwrap_or_default();

		let rows = if self.primitive == StatsPrimitive::Flow {
			self.aggregate_flow_rows(txn, all)?
		} else {
			self.collect_simple_rows(txn, all)?
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

impl SystemMetricsCdc {
	fn collect_simple_rows(
		&self,
		txn: &mut Transaction<'_>,
		entries: Vec<(MetricId, CdcStats)>,
	) -> Result<Vec<CdcRow>> {
		let mut rows = Vec::new();
		for (metric_id, stats) in entries {
			if let Some(row) = self.primitive.match_metric_id(txn, metric_id)? {
				rows.push((
					row.id,
					row.namespace_id,
					stats.key_bytes,
					stats.value_bytes,
					stats.total_bytes(),
					stats.entry_count,
				));
			}
		}
		Ok(rows)
	}

	fn aggregate_flow_rows(
		&self,
		txn: &mut Transaction<'_>,
		entries: Vec<(MetricId, CdcStats)>,
	) -> Result<Vec<CdcRow>> {
		let mut aggregated: HashMap<(u64, u64), CdcStats> = HashMap::new();
		for (metric_id, stats) in entries {
			if let Some(row) = self.primitive.match_metric_id(txn, metric_id)? {
				let entry = aggregated.entry((row.id, row.namespace_id)).or_default();
				*entry += stats;
			}
		}
		Ok(aggregated
			.into_iter()
			.map(|((id, namespace_id), stats)| {
				(
					id,
					namespace_id,
					stats.key_bytes,
					stats.value_bytes,
					stats.total_bytes(),
					stats.entry_count,
				)
			})
			.collect())
	}
}

fn build_columns(rows: Vec<CdcRow>) -> Columns {
	let capacity = rows.len();
	let mut ids = ColumnBuffer::uint8_with_capacity(capacity);
	let mut namespace_ids = ColumnBuffer::uint8_with_capacity(capacity);
	let mut key_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut value_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut total_bytes = ColumnBuffer::uint8_with_capacity(capacity);
	let mut counts = ColumnBuffer::uint8_with_capacity(capacity);

	for row in rows {
		ids.push(row.0);
		namespace_ids.push(row.1);
		key_bytes.push(row.2);
		value_bytes.push(row.3);
		total_bytes.push(row.4);
		counts.push(row.5);
	}

	Columns::new(vec![
		ColumnWithName::new(Fragment::internal("id"), ids),
		ColumnWithName::new(Fragment::internal("namespace_id"), namespace_ids),
		ColumnWithName::new(Fragment::internal("key_bytes"), key_bytes),
		ColumnWithName::new(Fragment::internal("value_bytes"), value_bytes),
		ColumnWithName::new(Fragment::internal("total_bytes"), total_bytes),
		ColumnWithName::new(Fragment::internal("count"), counts),
	])
}
