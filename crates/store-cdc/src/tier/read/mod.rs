// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read tier: an LRU of decoded blocks. A miss inflates one whole block rather than one record, because a
//! consumer that asked for version N asks for N+1 next, so the block it landed in is the block it will keep
//! reading. Residency is charged in bytes against a budget the store configures.

mod lookup;
mod mutate;
mod shard;

use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	sync::Arc,
};

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	tier::read::shard::{CdcReadBufferTierInner, Shard, build_shards},
	types::BlockId,
};

#[derive(Clone, Copy, Debug)]
pub struct CdcReadConfig {
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl Default for CdcReadConfig {
	fn default() -> Self {
		Self {
			resident_bytes: Some(ByteSize::from_mib(256)),
			shards: 8,
		}
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CdcReadMetrics {
	pub hits: u64,
	pub misses: u64,
	pub insertions: u64,
	pub evictions: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct CdcReadShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub blocks: usize,
	pub counters: CdcReadMetrics,
}

#[derive(Clone)]
pub struct CdcReadBufferTier {
	inner: Arc<CdcReadBufferTierInner>,
}

impl CdcReadBufferTier {
	#[instrument(name = "store::cdc::read::new", level = "debug", skip_all, fields(shards = config.shards))]
	pub fn new(config: CdcReadConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(CdcReadBufferTierInner {
				shards: build_shards(config, resident_bytes),
			}),
		})
	}

	#[instrument(name = "store::cdc::read::resident_bytes", level = "trace", skip(self))]
	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.all_shards().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
	}

	#[instrument(name = "store::cdc::read::shard_metrics", level = "trace", skip(self))]
	pub fn shard_metrics(&self) -> Vec<CdcReadShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			out.push(CdcReadShardMetrics {
				shard: index,
				used: shard.budget.used(),
				limit: shard.budget.limit(),
				blocks: shard.blocks.len(),
				counters: shard.metrics,
			});
		}
		out
	}

	fn shard_for(&self, id: BlockId) -> &Mutex<Shard> {
		let shards = &self.inner.shards;
		let mut hasher = DefaultHasher::new();
		id.hash(&mut hasher);
		let index = (hasher.finish() % shards.len() as u64) as usize;
		&shards[index]
	}

	fn all_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
		self.inner.shards.iter()
	}
}
