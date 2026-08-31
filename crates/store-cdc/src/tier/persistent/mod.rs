// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Persistent tier: append-only blocks under prefix truncation. A block is inserted once and never updated,
//! so the writer never rewrites a page a reader is holding, and retention only ever deletes off the front.

pub mod memory;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

use std::sync::Arc;

use reifydb_core::common::CommitVersion;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::{Result, byte_size::ByteSize, value::datetime::DateTime};

use crate::{
	storage::Cutoff,
	types::{Block, BlockId, BlockSummary, DropOutcome},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CdcPersistentMetrics {
	pub blocks: u64,
	pub stored_bytes: ByteSize,
	pub appends: u64,
	pub loads: u64,
	pub drops: u64,
}

#[repr(u8)]
#[derive(Clone)]
pub enum CdcPersistentTier {
	Memory(memory::MemoryCdcPersistent) = 0,
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(sqlite::SqliteCdcPersistent) = 1,
}

impl CdcPersistentTier {
	pub fn memory() -> Self {
		Self::Memory(memory::MemoryCdcPersistent::new())
	}

	pub fn is_resident(&self) -> bool {
		match self {
			Self::Memory(_) => true,
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(_) => false,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(sqlite::SqliteCdcPersistent::new(config))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = sqlite::SqliteCdcPersistent::in_memory();
		(Self::Sqlite(storage), guard)
	}

	/// Writes one block. Never called concurrently with itself; the flusher is the only writer.
	pub fn append_block(&self, block: &Block) -> Result<()> {
		match self {
			Self::Memory(storage) => storage.append_block(block),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.append_block(block),
		}
	}

	/// Inflates the block covering `version`, or `None` when no block does.
	pub fn load_block_containing(&self, version: CommitVersion) -> Result<Option<Arc<Block>>> {
		match self {
			Self::Memory(storage) => storage.load_block_containing(version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.load_block_containing(version),
		}
	}

	pub fn load_block(&self, id: BlockId) -> Result<Option<Arc<Block>>> {
		match self {
			Self::Memory(storage) => storage.load_block(id),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.load_block(id),
		}
	}

	/// Summaries only, ascending from the block covering `from`, without inflating any payload.
	pub fn summaries_from(&self, from: CommitVersion, limit: usize) -> Result<Vec<BlockSummary>> {
		match self {
			Self::Memory(storage) => storage.summaries_from(from, limit),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.summaries_from(from, limit),
		}
	}

	/// Deletes whole blocks whose highest version is below `cutoff`, at most `limit` of them. A block that
	/// straddles the cutoff is retained intact, so retention never rewrites a block.
	pub fn drop_blocks_below(&self, cutoff: Cutoff, limit: usize) -> Result<DropOutcome> {
		match self {
			Self::Memory(storage) => storage.drop_blocks_below(cutoff, limit),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.drop_blocks_below(cutoff, limit),
		}
	}

	pub fn min_version(&self) -> Result<Option<CommitVersion>> {
		match self {
			Self::Memory(storage) => storage.min_version(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.min_version(),
		}
	}

	pub fn max_version(&self) -> Result<Option<CommitVersion>> {
		match self {
			Self::Memory(storage) => storage.max_version(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.max_version(),
		}
	}

	/// Lowest version at or after `cutoff` by record timestamp, answered from block summaries alone;
	/// `Unbounded` when no record survives the cutoff, which no version can name.
	pub fn find_ttl_cutoff(&self, cutoff: DateTime) -> Result<Option<Cutoff>> {
		match self {
			Self::Memory(storage) => storage.find_ttl_cutoff(cutoff),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.find_ttl_cutoff(cutoff),
		}
	}

	pub fn truncated_before(&self) -> CommitVersion {
		match self {
			Self::Memory(storage) => storage.truncated_before(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.truncated_before(),
		}
	}

	pub fn metrics(&self) -> CdcPersistentMetrics {
		match self {
			Self::Memory(storage) => storage.metrics(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.metrics(),
		}
	}

	pub fn shutdown(&self) {
		match self {
			Self::Memory(storage) => storage.shutdown(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.shutdown(),
		}
	}
}
