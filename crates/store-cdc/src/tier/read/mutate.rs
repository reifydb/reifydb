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

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		common::{ChangeVersion, CommitVersion},
		interface::cdc::Cdc,
	};
	use reifydb_value::{byte_size::ByteSize, count::Count, value::datetime::DateTime};

	use crate::{
		tier::read::{CdcReadBufferTier, CdcReadConfig},
		types::{Block, BlockId, BlockSummary, cdc_resident_bytes},
	};

	fn block(version: u64, entries: u64) -> Arc<Block> {
		let commit = CommitVersion(version);
		let cdcs: Vec<Arc<Cdc>> = (0..entries)
			.map(|offset| {
				Arc::new(Cdc::new(
					ChangeVersion::from(CommitVersion(version - entries + 1 + offset)),
					DateTime::from_nanos(version as i64),
					vec![],
				))
			})
			.collect();
		Arc::new(Block::new(
			BlockSummary {
				id: BlockId(commit),
				min_version: CommitVersion(version - entries + 1),
				max_version: commit,
				min_timestamp: DateTime::from_nanos(version as i64),
				max_timestamp: DateTime::from_nanos(version as i64),
				count: Count::new(entries),
				stored_bytes: ByteSize::ZERO,
			},
			cdcs,
		))
	}

	fn tier(bytes: u64) -> CdcReadBufferTier {
		CdcReadBufferTier::new(CdcReadConfig {
			resident_bytes: Some(ByteSize::from_bytes(bytes)),
			shards: 1,
		})
		.expect("a read tier with a byte budget must be constructed")
	}

	#[test]
	fn a_cached_block_is_charged_exactly_the_bytes_of_its_entries() {
		// a resident size computed once at build must still equal a full walk of the entries, or the budget
		// drifts.
		let tier = tier(1 << 20);
		let block = block(20, 8);
		let walked = block
			.entries
			.iter()
			.fold(ByteSize::ZERO, |acc, cdc| acc.saturating_add(cdc_resident_bytes(cdc)));

		tier.insert(Arc::clone(&block));

		assert_eq!(block.resident_bytes(), walked);
		assert_eq!(tier.resident_bytes(), walked, "the read tier must charge the block's entry bytes");
		assert_eq!(tier.shard_metrics()[0].counters.insertions, 1);
	}
}
