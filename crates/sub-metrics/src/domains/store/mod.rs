// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_store_multi::{
	MultiStore,
	tier::{commit::buffer::MultiCommitMetrics, persistent::SqlitePageCacheMetrics, read::ReadBufferShardMetrics},
};
use reifydb_store_operator::{
	store::OperatorStore,
	tier::{
		persistent::OperatorPageCacheMetrics,
		read::{OperatorReadBufferKeyspaceMetrics, OperatorReadBufferShardMetrics},
	},
};
use reifydb_store_single::{
	SingleStore,
	tier::{commit::buffer::SingleCommitMetrics, persistent::SinglePageCacheMetrics},
};

#[derive(Clone)]
pub struct StoreReader {
	multi: MultiStore,
	single: SingleStore,
	operator: OperatorStore,
}

impl StoreReader {
	pub fn new(multi: MultiStore, single: SingleStore, operator: OperatorStore) -> Self {
		Self {
			multi,
			single,
			operator,
		}
	}

	pub fn multi_commit(&self) -> MultiCommitMetrics {
		self.multi.commit_metrics()
	}

	pub fn multi_read(&self) -> Vec<ReadBufferShardMetrics> {
		self.multi.read_buffer_shard_metrics()
	}

	pub fn multi_persistent(&self) -> Option<SqlitePageCacheMetrics> {
		self.multi.persistent_page_cache_metrics()
	}

	pub fn single_commit(&self) -> Option<SingleCommitMetrics> {
		self.single.commit_metrics()
	}

	pub fn single_persistent(&self) -> Option<SinglePageCacheMetrics> {
		self.single.persistent_page_cache_metrics()
	}

	pub fn operator_read(&self) -> Vec<OperatorReadBufferShardMetrics> {
		self.operator.read_buffer_shard_metrics()
	}

	pub fn operator_read_by_keyspace(&self) -> Vec<OperatorReadBufferKeyspaceMetrics> {
		self.operator.read_buffer_keyspace_metrics()
	}

	pub fn operator_persistent(&self) -> Option<OperatorPageCacheMetrics> {
		self.operator.persistent_page_cache_metrics()
	}
}
