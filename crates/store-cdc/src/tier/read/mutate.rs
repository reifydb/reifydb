// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use reifydb_core::common::CommitVersion;
use tracing::instrument;

use crate::{
	tier::read::{CdcReadBufferTier, shard::Resident},
	types::Block,
};

impl CdcReadBufferTier {
	#[instrument(name = "store::cdc::read::insert", level = "trace", skip_all, fields(block = block.id().0.0))]
	pub fn insert(&self, block: Arc<Block>) {
		let bytes = block.resident_bytes();
		let version = block.max_version();
		let mut shard = self.shard_for(block.id()).lock();
		let tick = shard.touch();
		let replaced = shard.blocks.insert(
			version,
			Resident {
				block,
				bytes,
				tick,
			},
		);
		if let Some(previous) = replaced {
			shard.budget.release(previous.bytes);
		}
		shard.budget.charge(bytes);
		shard.metrics.insertions += 1;
		shard.evict_to_capacity();
	}

	#[instrument(name = "store::cdc::read::clear", level = "debug", skip(self))]
	pub fn clear(&self) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			let dropped = mem::take(&mut shard.blocks);
			for resident in dropped.into_values() {
				shard.budget.release(resident.bytes);
			}
		}
	}

	#[instrument(name = "store::cdc::read::invalidate_below", level = "debug", skip(self), fields(version = version.0))]
	pub fn invalidate_below(&self, version: CommitVersion) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			let retained = shard.blocks.split_off(&version);
			let dropped = mem::replace(&mut shard.blocks, retained);
			for resident in dropped.into_values() {
				shard.budget.release(resident.bytes);
			}
		}
	}
}
