// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::metrics::{collect::MetricsCollector, sample::MetricsSample};
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	store::CdcStore,
	tier::{
		commit::CdcCommitMetrics,
		persistent::CdcPersistentMetrics,
		read::{CdcReadBufferTier, CdcReadShardMetrics},
	},
};

const CDC_COMMIT_SCOPE: &str = "store_cdc::commit";
const CDC_READ_SCOPE: &str = "store_cdc::read";

impl CdcStore {
	#[instrument(name = "store::cdc::commit_metrics", level = "trace", skip(self))]
	pub fn commit_metrics(&self) -> CdcCommitMetrics {
		self.commit.metrics()
	}

	#[instrument(name = "store::cdc::read_buffer_shard_metrics", level = "trace", skip(self))]
	pub fn read_buffer_shard_metrics(&self) -> Vec<CdcReadShardMetrics> {
		self.read.as_ref().map(CdcReadBufferTier::shard_metrics).unwrap_or_default()
	}

	#[instrument(name = "store::cdc::persistent_metrics", level = "trace", skip(self))]
	pub fn persistent_metrics(&self) -> CdcPersistentMetrics {
		self.persistent.metrics()
	}

	pub fn persistent_is_resident(&self) -> bool {
		self.persistent.is_resident()
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		vec![Arc::new(self.clone())]
	}

	#[instrument(name = "store::cdc::resident_bytes", level = "trace", skip(self))]
	pub fn resident_bytes(&self) -> ByteSize {
		let read = self.read.as_ref().map(CdcReadBufferTier::resident_bytes).unwrap_or(ByteSize::ZERO);
		self.commit.resident_bytes().saturating_add(read)
	}
}

impl MetricsCollector for CdcStore {
	#[instrument(name = "store::cdc::metrics_collect", level = "debug", skip_all)]
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let commit = self.commit_metrics();
		out.push(MetricsSample::heap(CDC_COMMIT_SCOPE, "resident_bytes", commit.resident_bytes));
		out.push(MetricsSample::count(CDC_COMMIT_SCOPE, "resident_entries", commit.entries.as_u64()));

		let shards = self.read_buffer_shard_metrics();
		let used = shards.iter().map(|shard| shard.used.as_bytes()).sum();
		let blocks = shards.iter().map(|shard| shard.blocks as u64).sum();
		out.push(MetricsSample::heap(CDC_READ_SCOPE, "resident_bytes", ByteSize::from_bytes(used)));
		out.push(MetricsSample::count(CDC_READ_SCOPE, "resident_blocks", blocks));
	}
}
