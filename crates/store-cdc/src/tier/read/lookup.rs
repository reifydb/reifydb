// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::common::CommitVersion;
use tracing::instrument;

use crate::{
	tier::read::CdcReadBufferTier,
	types::{Block, BlockId},
};

impl CdcReadBufferTier {
	#[instrument(name = "store::cdc::read::block_containing", level = "trace", skip(self), fields(version = version.0))]
	pub fn block_containing(&self, version: CommitVersion) -> Option<Arc<Block>> {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			let tick = shard.touch();
			let found = match shard.blocks.range_mut(version..).next() {
				Some((_, resident)) if resident.block.contains(version) => {
					resident.tick = tick;
					Some(resident.block.clone())
				}
				_ => None,
			};
			if let Some(block) = found {
				shard.metrics.hits += 1;
				return Some(block);
			}
		}
		let mut shard = self.shard_for(BlockId(version)).lock();
		shard.metrics.misses += 1;
		None
	}
}
