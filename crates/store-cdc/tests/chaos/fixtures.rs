// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The persistent tier and read-buffer budget must never change what a read answers; only the cut size may move block
//! boundaries.

use std::collections::BTreeSet;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		cdc::{Cdc, CdcChange},
	},
	key::row::RowKey,
};
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
	types::cdc_resident_bytes,
};
use reifydb_value::{
	byte_size::ByteSize,
	util::cowvec::CowVec,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use crate::oracle::{Oracle, Record};

/// High enough that an append never stalls waiting on the flusher, which would make the run depend on timing.
pub const CEILING: ByteSize = ByteSize::from_mib(64);

/// Small enough that a handful of records fills it, so one flush cuts several blocks.
pub const CUT_SMALL: ByteSize = ByteSize::from_bytes(512);

pub const CUT_MEDIUM: ByteSize = ByteSize::from_kib(4);

/// Larger than anything a run can buffer, so blocks are exactly the runs between explicit flushes.
pub const CUT_LARGE: ByteSize = ByteSize::from_mib(4);

const SUMMARY_LIMIT: usize = 1 << 16;

pub struct Config {
	pub name: &'static str,
	pub store: CdcStore,
	pub persistent: CdcPersistentTier,
	pub cut_bytes: ByteSize,
	pub oracle: Oracle,
	pub floor_seen: u64,
	read: Option<CdcReadConfig>,
	spawner: ActorSpawner,
	_guard: Option<SqliteTempPathGuard>,
}

impl Config {
	/// Rebuilds the facade with a fresh commit buffer and a cold read buffer, matching what a boot sees: nothing
	/// still buffered survives.
	pub fn reopen(&mut self) {
		self.store = build_store(&self.spawner, &self.persistent, self.read, self.cut_bytes);
		self.oracle.reopen();
	}

	pub fn summaries(&self) -> Vec<(u64, u64, u64, u64, u64)> {
		self.persistent
			.summaries_from(CommitVersion(0), SUMMARY_LIMIT)
			.expect("a healthy persistent tier must answer for its block layout")
			.into_iter()
			.map(|summary| {
				(
					summary.min_version.0,
					summary.max_version.0,
					summary.count.as_u64(),
					summary.min_timestamp.to_nanos(),
					summary.max_timestamp.to_nanos(),
				)
			})
			.collect()
	}
}

pub struct Harness {
	pub configs: Vec<Config>,
}

impl Harness {
	pub fn new() -> Self {
		let spawner = spawner();
		Self {
			configs: vec![
				config("memory", &spawner, memory_tier(), None, CUT_SMALL),
				config(
					"memory_cached",
					&spawner,
					memory_tier(),
					Some(CdcReadConfig::default()),
					CUT_LARGE,
				),
				config("sqlite", &spawner, sqlite_tier(), None, CUT_SMALL),
				config(
					"sqlite_cached",
					&spawner,
					sqlite_tier(),
					Some(CdcReadConfig::default()),
					CUT_MEDIUM,
				),
				config("sqlite_starved", &spawner, sqlite_tier(), Some(starved_read()), CUT_LARGE),
				config("sqlite_evicting", &spawner, sqlite_tier(), Some(evicting_read()), CUT_SMALL),
			],
		}
	}

	pub fn flush_all(&mut self) {
		for config in &mut self.configs {
			flush(config);
		}
	}
}

/// Drains the commit tier and the model together, so afterward both agree on block layout, not merely the records.
pub fn flush(config: &mut Config) {
	assert!(
		config.store.flush_pending(),
		"config={} reported a failed flush, so the flush actor is not answering",
		config.name
	);
	config.oracle.flush();
}

/// Runs the same flush the actor runs, but calls `observe` inside the window where a batch has left the commit buffer
/// and has not yet reached the persistent tier, the one state where a record can fall into the gap or be served twice.
pub fn flush_staged(config: &mut Config, observe: impl Fn(&Config)) {
	{
		let view: &Config = config;
		view.store.flush_staged(&mut || observe(view));
	}
	config.oracle.flush();
}

/// Leaked so the flush actor outlives the borrow; the flush interval is parked an hour out so only an explicit flush
/// ever seals a block.
pub fn spawner() -> ActorSpawner {
	let actor_system = ActorSystem::new(Pools::new(PoolConfig::sync_only()), Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	spawner
}

pub fn memory_tier() -> (CdcPersistentTier, Option<SqliteTempPathGuard>) {
	(CdcPersistentTier::memory(), None)
}

pub fn sqlite_tier() -> (CdcPersistentTier, Option<SqliteTempPathGuard>) {
	let (config, guard) = SqliteConfig::in_memory();
	(CdcPersistentTier::sqlite(config), Some(guard))
}

/// A budget no block can fit under, so every insert is evicted immediately and the read buffer is never a source of
/// truth.
pub fn starved_read() -> CdcReadConfig {
	CdcReadConfig {
		resident_bytes: Some(ByteSize::from_bytes(1)),
		shards: 1,
	}
}

/// Holds a handful of blocks on one shard, so a victim is picked under real pressure instead of every insert being
/// evicted on the spot; a single shard keeps the budget the whole tier's, not a fraction of it.
pub fn evicting_read() -> CdcReadConfig {
	CdcReadConfig {
		resident_bytes: Some(ByteSize::from_bytes(CUT_SMALL.as_bytes() * 8)),
		shards: 1,
	}
}

pub fn config(
	name: &'static str,
	spawner: &ActorSpawner,
	tier: (CdcPersistentTier, Option<SqliteTempPathGuard>),
	read: Option<CdcReadConfig>,
	cut_bytes: ByteSize,
) -> Config {
	let (persistent, guard) = tier;
	let store = build_store(spawner, &persistent, read, cut_bytes);
	Config {
		name,
		store,
		persistent,
		cut_bytes,
		oracle: Oracle::new(cut_bytes.as_bytes()),
		floor_seen: 0,
		read,
		spawner: spawner.clone(),
		_guard: guard,
	}
}

pub fn build_store(
	spawner: &ActorSpawner,
	persistent: &CdcPersistentTier,
	read: Option<CdcReadConfig>,
	cut_bytes: ByteSize,
) -> CdcStore {
	CdcStore::new(CdcStoreConfig {
		commit: CdcCommitConfig {
			storage: CdcCommitBufferTier::new(cut_bytes, CEILING),
			cut_bytes,
			ceiling: CEILING,
		},
		persistent: CdcPersistentConfig::opened(persistent.clone())
			.flush_interval(Duration::from_hours_const(1)),
		read,
		spawner: spawner.clone(),
		clock: Clock::Real,
	})
}

/// Which variant a change carries. The eviction rollup charges `value_bytes` per variant, so a suite that only ever
/// inserts leaves two thirds of that accounting unmeasured.
#[derive(Clone, Copy, Debug)]
pub enum ChangeKind {
	Insert,
	Update,
	Delete {
		pre: bool,
		visible: bool,
	},
}

/// Builds one record and its model row; changes carry real row keys so a drop's eviction rollup resolves per table
/// instead of one bucket.
pub fn record(version: u64, timestamp: u64, changes: &[(u64, u64, usize, ChangeKind)]) -> (Cdc, Record) {
	let mut list = Vec::new();
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	let mut sources: BTreeSet<u64> = BTreeSet::new();
	for (table, row, len, kind) in changes {
		let key = RowKey::encoded(StorageId::table(TableId(*table)), RowNumber(*row));
		// pre and post differ so a tier that returned one in place of the other cannot pass the payload check
		let change = match kind {
			ChangeKind::Insert => CdcChange::Insert {
				key,
				post: payload(*table, *len),
			},
			ChangeKind::Update => CdcChange::Update {
				key,
				pre: payload(*table, *len),
				post: payload(table.wrapping_add(1), *len),
			},
			ChangeKind::Delete {
				pre,
				visible,
			} => CdcChange::Delete {
				key,
				pre: pre.then(|| payload(*table, *len)),
				visible: *visible,
			},
		};
		key_bytes += change.key().len() as u64;
		value_bytes += change.value_bytes() as u64;
		sources.insert(*table);
		list.push(change);
	}
	let cdc = Cdc::new(CommitVersion(version), DateTime::from_nanos(timestamp), list);
	let record = Record {
		changes: cdc.changes.clone(),
		timestamp,
		bytes: cdc_resident_bytes(&cdc).as_bytes(),
		key_bytes,
		value_bytes,
		sources,
	};
	(cdc, record)
}

fn payload(seed: u64, len: usize) -> EncodedBytes {
	EncodedBytes(CowVec::new(vec![(seed % 251) as u8; len]))
}
