// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

use crate::tier::{commit::CdcCommitBufferTier, persistent::CdcPersistentTier, read::CdcReadConfig};

pub const BLOCK_CUT_BYTES: ByteSize = ByteSize::from_mib(4);

pub const FLUSH_INTERVAL: Duration = Duration::from_seconds_const(1);

pub const COMMIT_BUFFER_CEILING: ByteSize = ByteSize::from_mib(256);

#[derive(Clone)]
pub struct CdcCommitConfig {
	pub storage: CdcCommitBufferTier,
	pub cut_bytes: ByteSize,
	pub ceiling: ByteSize,
}

impl Default for CdcCommitConfig {
	fn default() -> Self {
		Self {
			storage: CdcCommitBufferTier::new(BLOCK_CUT_BYTES, COMMIT_BUFFER_CEILING),
			cut_bytes: BLOCK_CUT_BYTES,
			ceiling: COMMIT_BUFFER_CEILING,
		}
	}
}

#[derive(Clone)]
pub struct CdcPersistentConfig {
	pub storage: CdcPersistentTier,
	pub flush_interval: Duration,
}

impl CdcPersistentConfig {
	pub fn memory() -> Self {
		Self::opened(CdcPersistentTier::memory())
	}

	pub fn opened(storage: CdcPersistentTier) -> Self {
		Self {
			storage,
			flush_interval: FLUSH_INTERVAL,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::opened(CdcPersistentTier::sqlite(config))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = CdcPersistentTier::sqlite_in_memory();
		(Self::opened(storage), guard)
	}

	pub fn flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval = interval;
		self
	}
}

#[derive(Clone)]
pub struct CdcStoreConfig {
	pub commit: CdcCommitConfig,
	pub persistent: CdcPersistentConfig,
	pub read: Option<CdcReadConfig>,
	pub spawner: ActorSpawner,
	pub clock: Clock,
}

impl CdcStoreConfig {
	pub fn memory(spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			commit: CdcCommitConfig::default(),
			persistent: CdcPersistentConfig::memory(),
			read: None,
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: CdcPersistentConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			commit: CdcCommitConfig::default(),
			persistent,
			read: Some(CdcReadConfig::default()),
			spawner,
			clock,
		}
	}
}
