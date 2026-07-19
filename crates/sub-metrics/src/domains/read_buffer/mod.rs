// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::user::UserVTableColumn;
use reifydb_core::{
	interface::catalog::id::NamespaceId,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_store_multi::{MultiStore, tier::read::ReadBufferShardMetrics};
use reifydb_value::{
	fragment::Fragment,
	value::{datetime::DateTime, value_type::ValueType},
};

use crate::framework::source::MetricsSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadBufferDomain {
	Shards,
	Warms,
	Reads,
}

impl ReadBufferDomain {
	pub const ALL: [ReadBufferDomain; 3] =
		[ReadBufferDomain::Shards, ReadBufferDomain::Warms, ReadBufferDomain::Reads];

	pub fn namespace(&self) -> NamespaceId {
		match self {
			ReadBufferDomain::Shards => NamespaceId::SYSTEM_METRICS_READ_BUFFER_SHARDS,
			ReadBufferDomain::Warms => NamespaceId::SYSTEM_METRICS_READ_BUFFER_WARMS,
			ReadBufferDomain::Reads => NamespaceId::SYSTEM_METRICS_READ_BUFFER_READS,
		}
	}

	pub fn columns(&self) -> Vec<UserVTableColumn> {
		let mut columns = vec![
			UserVTableColumn::new("ts", ValueType::DateTime),
			UserVTableColumn::new("domain", ValueType::Utf8),
			UserVTableColumn::new("shard", ValueType::Uint2),
		];
		let names: &[&str] = match self {
			ReadBufferDomain::Shards => &[
				"used",
				"limit",
				"pages",
				"page_cap",
				"payload",
				"entries",
				"hot_pages",
				"complete_pages",
				"blocked_pages",
				"warming",
			],
			ReadBufferDomain::Warms => &[
				"started",
				"completed",
				"dirty_aborted",
				"aborted",
				"blocked_marks",
				"evicted_pages",
				"invalidated_complete_pages",
			],
			ReadBufferDomain::Reads => {
				&["point_hits", "previous_hits", "point_misses", "range_served", "range_gaps"]
			}
		};
		for name in names {
			columns.push(UserVTableColumn::new(*name, ValueType::Uint8));
		}
		columns
	}

	fn values(&self, metrics: &ReadBufferShardMetrics) -> Vec<u64> {
		match self {
			ReadBufferDomain::Shards => vec![
				metrics.state.used.as_bytes(),
				metrics.state.limit.as_bytes(),
				metrics.state.pages as u64,
				metrics.state.page_cap as u64,
				metrics.state.payload.as_bytes(),
				metrics.state.entries as u64,
				metrics.state.hot_pages as u64,
				metrics.state.complete_pages as u64,
				metrics.state.blocked_pages as u64,
				metrics.state.warming as u64,
			],
			ReadBufferDomain::Warms => vec![
				metrics.warms.warms_started,
				metrics.warms.warms_completed,
				metrics.warms.warms_dirty_aborted,
				metrics.warms.warms_aborted,
				metrics.warms.pages_warm_blocked,
				metrics.warms.pages_evicted,
				metrics.warms.complete_pages_invalidated,
			],
			ReadBufferDomain::Reads => vec![
				metrics.reads.point_hits,
				metrics.reads.previous_hits,
				metrics.reads.point_misses,
				metrics.reads.range_served,
				metrics.reads.range_gaps,
			],
		}
	}
}

pub struct ReadBufferSource {
	domain: ReadBufferDomain,
	store: MultiStore,
}

impl MetricsSource for ReadBufferSource {
	fn namespace(&self) -> NamespaceId {
		self.domain.namespace()
	}

	fn columns(&self) -> Vec<UserVTableColumn> {
		self.domain.columns()
	}

	fn collect(&self, now: DateTime) -> Columns {
		let shards = self.store.read_buffer_shard_metrics();
		let capacity = shards.len();
		let spec = self.domain.columns();

		let mut ts = ColumnBuffer::datetime_with_capacity(capacity);
		let mut domain = ColumnBuffer::utf8_with_capacity(capacity);
		let mut shard = ColumnBuffer::uint2_with_capacity(capacity);
		let mut buffers: Vec<ColumnBuffer> =
			spec.iter().skip(3).map(|_| ColumnBuffer::uint8_with_capacity(capacity)).collect();

		for metrics in &shards {
			ts.push(now);
			domain.push(metrics.domain);
			shard.push(metrics.shard as u16);
			for (buffer, value) in buffers.iter_mut().zip(self.domain.values(metrics)) {
				buffer.push(value);
			}
		}

		let mut out = vec![
			ColumnWithName::new(Fragment::internal("ts"), ts),
			ColumnWithName::new(Fragment::internal("domain"), domain),
			ColumnWithName::new(Fragment::internal("shard"), shard),
		];
		for (column, buffer) in spec.into_iter().skip(3).zip(buffers) {
			out.push(ColumnWithName::new(Fragment::internal(column.name), buffer));
		}
		Columns::new(out)
	}
}

pub fn read_buffer_sources(store: &MultiStore) -> Vec<Arc<dyn MetricsSource>> {
	ReadBufferDomain::ALL
		.iter()
		.map(|&domain| {
			Arc::new(ReadBufferSource {
				domain,
				store: store.clone(),
			}) as Arc<dyn MetricsSource>
		})
		.collect()
}
