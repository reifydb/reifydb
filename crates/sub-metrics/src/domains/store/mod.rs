// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_filter::adaptive::FilterMetrics;
use reifydb_store::metrics::PageCacheMetrics;
use reifydb_store_cdc::{
	store::CdcStore,
	tier::{commit::CdcCommitMetrics, persistent::CdcPersistentMetrics, read::CdcReadShardMetrics},
};
use reifydb_store_multi::{
	MultiStore,
	store::MultiPersistentProbeMetrics,
	tier::{
		commit::buffer::MultiCommitMetrics,
		point::MultiPointShardMetrics,
		range::MultiRangeShardMetrics,
	},
};
use reifydb_store_operator::{
	store::OperatorStore,
	tier::{
		point::{OperatorPointKeyspaceMetrics, OperatorPointShardMetrics},
		range::{OperatorRangeKeyspaceMetrics, OperatorRangeShardMetrics},
	},
};
use reifydb_store_single::{
	SingleStore,
	store::SinglePersistentProbeMetrics,
	tier::{commit::buffer::SingleCommitMetrics, persistent::SinglePageCacheMetrics},
};

#[derive(Clone)]
pub struct StoreReader {
	multi: MultiStore,
	single: SingleStore,
	operator: OperatorStore,
	cdc: CdcStore,
}

impl StoreReader {
	pub fn new(multi: MultiStore, single: SingleStore, operator: OperatorStore, cdc: CdcStore) -> Self {
		Self {
			multi,
			single,
			operator,
			cdc,
		}
	}

	pub fn multi_commit(&self) -> MultiCommitMetrics {
		self.multi.commit_metrics()
	}

	pub fn multi_byte_split(&self) -> (usize, usize, usize, usize, usize, usize, usize, usize, usize, usize) {
		self.multi.debug_byte_split()
	}

	pub fn multi_key_overlap(&self) -> (usize, usize, usize, usize, usize) {
		self.multi.debug_key_overlap()
	}

	pub fn multi_point(&self) -> Vec<MultiPointShardMetrics> {
		self.multi.point_shard_metrics()
	}

	pub fn multi_range(&self) -> Vec<MultiRangeShardMetrics> {
		self.multi.range_shard_metrics()
	}

	pub fn multi_persistent(&self) -> Option<PageCacheMetrics> {
		self.multi.persistent_page_cache_metrics()
	}

	pub fn multi_persistent_probe(&self) -> Option<MultiPersistentProbeMetrics> {
		self.multi.persistent_probe_metrics()
	}

	pub fn multi_filter(&self) -> Option<FilterMetrics> {
		self.multi.persistent_filter_metrics()
	}

	pub fn single_commit(&self) -> Option<SingleCommitMetrics> {
		self.single.commit_metrics()
	}

	pub fn single_persistent(&self) -> Option<SinglePageCacheMetrics> {
		self.single.persistent_page_cache_metrics()
	}

	pub fn single_persistent_probe(&self) -> Option<SinglePersistentProbeMetrics> {
		self.single.persistent_probe_metrics()
	}

	pub fn operator_point(&self) -> Vec<OperatorPointShardMetrics> {
		self.operator.point_shard_metrics()
	}

	pub fn operator_point_by_keyspace(&self) -> Vec<OperatorPointKeyspaceMetrics> {
		self.operator.point_keyspace_metrics()
	}

	pub fn operator_range(&self) -> Vec<OperatorRangeShardMetrics> {
		self.operator.range_shard_metrics()
	}

	pub fn operator_range_by_keyspace(&self) -> Vec<OperatorRangeKeyspaceMetrics> {
		self.operator.range_keyspace_metrics()
	}

	pub fn operator_persistent(&self) -> Option<PageCacheMetrics> {
		self.operator.persistent_page_cache_metrics()
	}

	pub fn operator_filter(&self) -> Option<FilterMetrics> {
		self.operator.persistent_filter_metrics()
	}

	pub fn cdc_commit(&self) -> CdcCommitMetrics {
		self.cdc.commit_metrics()
	}

	pub fn cdc_read(&self) -> Vec<CdcReadShardMetrics> {
		self.cdc.read_buffer_shard_metrics()
	}

	pub fn cdc_persistent(&self) -> CdcPersistentMetrics {
		self.cdc.persistent_metrics()
	}
}
