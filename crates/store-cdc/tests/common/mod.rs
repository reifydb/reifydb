// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

pub mod runner;

use reifydb_runtime::{
	actor::system::{ActorSpawner, ActorSystem},
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_cdc::{
	config::{CdcCommitConfig, CdcPersistentConfig, CdcStoreConfig},
	store::CdcStore,
	tier::{commit::CdcCommitBufferTier, persistent::CdcPersistentTier, read::CdcReadConfig},
};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

/// The persistent tier is handed out alongside the store so a test can read the sealed block layout without the facade
/// growing an accessor no production caller wants.
pub struct Fixture {
	pub store: CdcStore,
	pub persistent: CdcPersistentTier,
	pub guard: Option<SqliteTempPathGuard>,
}

fn spawner() -> ActorSpawner {
	// the flusher needs a thread of its own, and the system must outlive the store the test holds
	let actor_system = ActorSystem::new(Pools::new(PoolConfig::default()), Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	spawner
}

fn build(persistent: CdcPersistentTier, read: Option<CdcReadConfig>, guard: Option<SqliteTempPathGuard>) -> Fixture {
	custom(persistent, read, CdcCommitConfig::default(), guard)
}

/// Builds a store over an already-opened persistent tier so a test can reopen it fresh or force a cut smaller than the
/// default four megabytes.
pub fn custom(
	persistent: CdcPersistentTier,
	read: Option<CdcReadConfig>,
	commit: CdcCommitConfig,
	guard: Option<SqliteTempPathGuard>,
) -> Fixture {
	// an hour-long interval keeps the timer from racing the test, so every block boundary comes from an explicit
	// flush
	let store = CdcStore::new(CdcStoreConfig {
		commit,
		persistent: CdcPersistentConfig::opened(persistent.clone())
			.flush_interval(Duration::from_hours_const(1)),
		read,
		spawner: spawner(),
		clock: Clock::Real,
	});
	Fixture {
		store,
		persistent,
		guard,
	}
}

pub fn commit_config(cut_bytes: ByteSize, ceiling: ByteSize) -> CdcCommitConfig {
	CdcCommitConfig {
		storage: CdcCommitBufferTier::new(cut_bytes, ceiling),
		cut_bytes,
		ceiling,
	}
}

pub fn memory() -> Fixture {
	build(CdcPersistentTier::memory(), None, None)
}

pub fn memory_cached() -> Fixture {
	build(CdcPersistentTier::memory(), Some(CdcReadConfig::default()), None)
}

pub fn sqlite() -> Fixture {
	let (config, guard) = SqliteConfig::in_memory();
	build(CdcPersistentTier::sqlite(config), None, Some(guard))
}

pub fn sqlite_cached() -> Fixture {
	let (config, guard) = SqliteConfig::in_memory();
	build(CdcPersistentTier::sqlite(config), Some(CdcReadConfig::default()), Some(guard))
}

pub fn sqlite_starved_cache() -> Fixture {
	// a budget no block can fit under means every insert is evicted immediately, so this row proves the read buffer
	// is never a source of truth
	let (config, guard) = SqliteConfig::in_memory();
	let read = CdcReadConfig {
		resident_bytes: Some(ByteSize::from_bytes(1)),
		shards: 1,
	};
	build(CdcPersistentTier::sqlite(config), Some(read), Some(guard))
}

#[macro_export]
macro_rules! tier_cases {
	($fresh:expr, [$($case:ident),+ $(,)?]) => {
		$(
			#[test]
			fn $case() {
				super::cases::$case($fresh());
			}
		)+
	};
}

#[macro_export]
macro_rules! tier_tests {
	([$($tier:ident = $fresh:expr),+ $(,)?], $cases:tt) => {
		$(
			mod $tier {
				use super::*;

				$crate::tier_cases!($fresh, $cases);
			}
		)+
	};
}
