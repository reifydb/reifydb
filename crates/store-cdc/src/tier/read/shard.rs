// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{common::CommitVersion, util::budget::MemoryBudget};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	tier::read::{CdcReadConfig, CdcReadMetrics},
	types::Block,
};

pub(crate) struct Resident {
	pub(crate) block: Arc<Block>,
	pub(crate) bytes: ByteSize,
	pub(crate) tick: u64,
}

pub(crate) struct Shard {
	pub(crate) blocks: BTreeMap<CommitVersion, Resident>,
	pub(crate) budget: MemoryBudget,
	pub(crate) next_tick: u64,
	pub(crate) metrics: CdcReadMetrics,
}

impl Shard {
	pub(crate) fn touch(&mut self) -> u64 {
		self.next_tick += 1;
		self.next_tick
	}

	fn pick_victim(&self) -> Option<CommitVersion> {
		self.blocks.iter().min_by_key(|(_, resident)| resident.tick).map(|(version, _)| *version)
	}

	#[instrument(name = "store::cdc::read::evict_to_capacity", level = "trace", skip_all)]
	pub(crate) fn evict_to_capacity(&mut self) {
		while self.budget.over_budget() {
			let Some(victim) = self.pick_victim() else {
				break;
			};
			let Some(resident) = self.blocks.remove(&victim) else {
				break;
			};
			self.budget.release(resident.bytes);
			self.metrics.evictions += 1;
		}
	}
}

pub(crate) struct CdcReadBufferTierInner {
	pub(crate) shards: Box<[Mutex<Shard>]>,
}

pub(crate) fn build_shards(config: CdcReadConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let total = resident_bytes.as_bytes();
	let base = total / shard_count as u64;
	let remainder = total % shard_count as u64;
	(0..shard_count)
		.map(|index| {
			let byte_cap = ByteSize::from_bytes((base + u64::from((index as u64) < remainder)).max(1));
			Mutex::new(Shard {
				blocks: BTreeMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				metrics: CdcReadMetrics::default(),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}
