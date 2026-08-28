// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Store-configuration builders and the key/row helpers every scenario shares.

use std::{cell::Cell, path::Path};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::key::operator_state::{GroupId, KeyspaceId, OperatorStateKey};
use reifydb_runtime::{
	actor::system::{ActorSpawner, ActorSystem},
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{point::OperatorPointConfig, range::OperatorRangeConfig},
};

/// Only data keyspaces, so every key the workload writes lands in a census bucket.
pub const KEYSPACES: [u8; 4] = [0x10, 0x11, 0x1D, 0x40];

pub struct Config {
	pub name: &'static str,
	pub store: OperatorStore,
	/// The memory tier answers `bytes`/`total_bytes` with zero by contract, so only layered stores can be held
	/// to the accounted value.
	pub accounts_bytes: bool,
	/// Flushed after every mutation, which keeps the commit buffer empty and drives reads through sqlite.
	pub eager: bool,
	/// A write-through tier has no commit buffer, so its checkpoint floor is the logical minimum.
	pub write_through: bool,
	/// False once a mutation lands and true again after a flush. The layered census sums sqlite and the buffer
	/// without an overlay, so a key rewritten or tombstoned in the buffer is double counted or still billed; the
	/// sum is only equal to the logical state while the buffer is empty.
	pub buffer_clean: Cell<bool>,
}

impl Config {
	pub fn census_exact(&self) -> bool {
		self.buffer_clean.get()
	}

	pub fn bytes_exact(&self) -> bool {
		self.accounts_bytes && self.buffer_clean.get()
	}
}

pub struct Harness {
	pub configs: Vec<Config>,
	_guards: Vec<SqliteTempPathGuard>,
}

impl Harness {
	pub fn new() -> Self {
		let spawner = spawner();
		let memory = OperatorStore::standard(OperatorStoreConfig::memory(spawner.clone(), Clock::Real));
		let (buffered, buffered_guard) = layered(&spawner);
		let (eager, eager_guard) = layered(&spawner);
		Self {
			configs: vec![
				Config {
					name: "memory",
					store: memory,
					accounts_bytes: false,
					eager: false,
					write_through: true,
					buffer_clean: Cell::new(true),
				},
				Config {
					name: "layered",
					store: buffered,
					accounts_bytes: true,
					eager: false,
					write_through: false,
					buffer_clean: Cell::new(true),
				},
				Config {
					name: "layered_eager",
					store: eager,
					accounts_bytes: true,
					eager: true,
					write_through: false,
					buffer_clean: Cell::new(true),
				},
			],
			_guards: vec![buffered_guard, eager_guard],
		}
	}

	/// Called after every mutation: only the eager configuration flushes, so the layered one keeps whatever buffer
	/// depth the seed happened to build up and its accounting reads stop being exact until the next flush.
	pub fn after_mutation(&self) {
		for config in &self.configs {
			if !config.write_through {
				config.buffer_clean.set(false);
			}
			if config.eager {
				Self::flush(config);
			}
		}
	}

	pub fn flush_all(&self) {
		for config in &self.configs {
			Self::flush(config);
		}
	}

	fn flush(config: &Config) {
		assert!(
			config.store.flush_pending_blocking(),
			"config={} reported a failed flush, so the flush actor is not answering",
			config.name
		);
		config.buffer_clean.set(true);
	}
}

/// One actor system per harness, leaked so the flush actor outlives the borrow; the flush interval is parked an
/// hour out so nothing but an explicit `flush_pending_blocking` ever moves the buffer and the run stays a pure
/// function of the seed.
pub fn spawner() -> ActorSpawner {
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	spawner
}

pub fn layered(spawner: &ActorSpawner) -> (OperatorStore, SqliteTempPathGuard) {
	let (config, guard) = SqliteConfig::in_memory();
	(store_from(spawner, config), guard)
}

pub fn store_at(spawner: &ActorSpawner, path: &Path) -> OperatorStore {
	store_from(spawner, SqliteConfig::new(path))
}

fn store_from(spawner: &ActorSpawner, config: SqliteConfig) -> OperatorStore {
	OperatorStore::standard(OperatorStoreConfig {
		point: Some(OperatorPointConfig::testing()),
		range: Some(OperatorRangeConfig::testing()),
		..OperatorStoreConfig::sqlite(OperatorPersistentConfig::sqlite(config), spawner.clone(), Clock::Real)
	})
}

pub fn key(group: u64, keyspace: u8, suffix: u64) -> EncodedKey {
	OperatorStateKey::inner_encoded(GroupId(group.into()), KeyspaceId(keyspace), (suffix as u16).to_be_bytes())
		.as_encoded()
		.clone()
}

pub fn row(operator: u64, suffix: u64, step: u32) -> EncodedPodRow {
	EncodedPodRow::new(format!("o{operator}/k{suffix}@{step}").as_bytes())
}
