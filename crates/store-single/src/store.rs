// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	mem,
	ops::{Bound, Deref},
	sync::Arc,
	time::Duration,
};

use reifydb_core::{
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::SingleVersionRow,
};
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorSpawner, ActorSystem},
	},
	context::clock::Clock,
	pool::{PoolConfig, Pools},
	reifydb_assertions,
	shutdown::Shutdown,
	sync::{mutex::Mutex, waiter::WaiterHandle},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::util::{cowvec::CowVec, hex};
use tracing::instrument;

use crate::{
	Result, SingleVersionBatch, SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
	SingleVersionRangeRev, SingleVersionRemove, SingleVersionSet, SingleVersionStore,
	buffer::tier::SingleBufferTier,
	config::{BufferConfig, SingleStoreConfig},
	flush::actor::FlushMessage,
	persistent::SinglePersistentTier,
	tier::{RangeCursor, TierStorage},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{config::PersistentConfig, flush::actor::FlushActor};

pub type DirtyMap = HashMap<EncodedKey, Option<CowVec<u8>>>;

#[derive(Clone)]
pub struct StandardSingleStore(Arc<StandardSingleStoreInner>);

pub struct StandardSingleStoreInner {
	pub(crate) buffer: Option<SingleBufferTier>,
	pub(crate) persistent: Option<SinglePersistentTier>,
	#[allow(dead_code)]
	pub(crate) flush_actor: Option<ActorRef<FlushMessage>>,
	pub(crate) dirty: Arc<Mutex<DirtyMap>>,
	_spawner: ActorSpawner,
}

impl StandardSingleStore {
	#[instrument(name = "store::single::new", level = "debug", skip(config), fields(
		has_buffer = config.buffer.is_some(),
		has_persistent = config.persistent.is_some(),
	))]
	pub fn new(config: SingleStoreConfig) -> Result<Self> {
		let buffer = config.buffer.map(|c| c.storage);
		let spawner = config.spawner.clone();
		let dirty: Arc<Mutex<DirtyMap>> = Arc::new(Mutex::new(HashMap::new()));

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush_actor) = {
			let persistent_cfg = config.persistent.clone();
			let persistent = persistent_cfg.as_ref().map(|c| c.storage.clone());
			let flush_actor = match (persistent.as_ref(), persistent_cfg.as_ref()) {
				(Some(p), Some(cfg)) => Some(FlushActor::spawn(
					&spawner,
					Arc::clone(&dirty),
					p.clone(),
					cfg.flush_interval,
				)),
				_ => None,
			};
			(persistent, flush_actor)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush_actor): (Option<SinglePersistentTier>, Option<ActorRef<FlushMessage>>) = {
			let _ = config.persistent;
			(None, None)
		};

		Ok(Self(Arc::new(StandardSingleStoreInner {
			buffer,
			persistent,
			flush_actor,
			dirty,
			_spawner: spawner,
		})))
	}

	pub fn buffer(&self) -> Option<&SingleBufferTier> {
		self.buffer.as_ref()
	}

	pub fn persistent(&self) -> Option<&SinglePersistentTier> {
		self.persistent.as_ref()
	}

	pub fn flush_pending_blocking(&self) {
		let Some(actor_ref) = self.flush_actor.as_ref() else {
			return;
		};

		if self.dirty.lock().is_empty() {
			return;
		}

		let waiter = Arc::new(WaiterHandle::new());
		let waiter_for_msg = Arc::clone(&waiter);
		if actor_ref
			.send_blocking(FlushMessage::FlushPending {
				waiter: waiter_for_msg,
			})
			.is_err()
		{
			return;
		}

		waiter.wait_timeout(Duration::from_secs(5));
	}
}

impl Deref for StandardSingleStore {
	type Target = StandardSingleStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Shutdown for StandardSingleStore {
	fn shutdown(&self) {
		if let Some(persistent) = self.persistent.as_ref() {
			persistent.shutdown();
		}
	}
}

impl StandardSingleStore {
	pub fn testing_memory() -> Self {
		let pools = Pools::new(PoolConfig::sync_only());
		let clock = Clock::testing();
		let actor_system = ActorSystem::new(pools, clock.clone());
		let spawner = actor_system.spawner();
		let store = Self::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: None,
			spawner,
			clock,
		})
		.unwrap();
		mem::forget(actor_system);
		store
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		let pools = Pools::new(PoolConfig::default());
		let clock = Clock::testing();
		let actor_system = ActorSystem::new(pools, clock.clone());
		let spawner = actor_system.spawner();
		let (persistent, guard) = PersistentConfig::sqlite_in_memory();
		let store = Self::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: Some(persistent),
			spawner,
			clock,
		})
		.unwrap();
		mem::forget(actor_system);
		(store, guard)
	}
}

impl SingleVersionGet for StandardSingleStore {
	#[instrument(name = "store::single::get", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref())))]
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>> {
		if let Some(buffer) = &self.buffer {
			match buffer.get_with_tombstone(key.as_ref())? {
				Some(Some(value)) => {
					return Ok(Some(SingleVersionRow {
						key: key.clone(),
						row: EncodedRow(value),
					}));
				}
				Some(None) => return Ok(None),
				None => {}
			}
		}

		if let Some(persistent) = &self.persistent
			&& let Some(value) = persistent.get(key.as_ref())?
		{
			return Ok(Some(SingleVersionRow {
				key: key.clone(),
				row: EncodedRow(value),
			}));
		}

		Ok(None)
	}
}

impl SingleVersionContains for StandardSingleStore {
	#[instrument(name = "store::single::contains", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref())), ret)]
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		if let Some(buffer) = &self.buffer {
			match buffer.get_with_tombstone(key.as_ref())? {
				Some(Some(_)) => return Ok(true),
				Some(None) => return Ok(false),
				None => {}
			}
		}

		if let Some(persistent) = &self.persistent
			&& persistent.contains(key.as_ref())?
		{
			return Ok(true);
		}

		Ok(false)
	}
}

impl SingleVersionCommit for StandardSingleStore {
	#[instrument(name = "store::single::commit", level = "debug", skip(self, deltas), fields(delta_count = deltas.len()))]
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		let entries = self.entries_from_deltas(&deltas);
		self.apply_to_tiers(entries)
	}
}

impl StandardSingleStore {
	#[inline]
	fn entries_from_deltas(&self, deltas: &CowVec<Delta>) -> Vec<(EncodedKey, Option<CowVec<u8>>)> {
		deltas.iter()
			.map(|delta| match delta {
				Delta::Set {
					key,
					row,
				} => (key.clone(), Some(CowVec::new(row.as_ref().to_vec()))),
				Delta::Unset {
					key,
					..
				}
				| Delta::Remove {
					key,
				}
				| Delta::Drop {
					key,
				} => (key.clone(), None),
			})
			.collect()
	}

	#[inline]
	fn apply_to_tiers(&self, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> Result<()> {
		if let Some(buffer) = &self.buffer {
			buffer.set(entries.clone())?;
			if self.persistent.is_some() {
				let mut dirty = self.dirty.lock();
				for (key, value) in entries {
					dirty.insert(key, value);
				}
			}
		} else if let Some(persistent) = &self.persistent {
			persistent.set(entries)?;
		}

		Ok(())
	}
}

impl SingleVersionSet for StandardSingleStore {}
impl SingleVersionRemove for StandardSingleStore {}

impl SingleVersionRange for StandardSingleStore {
	#[instrument(name = "store::single::range_batch", level = "debug", skip(self), fields(batch_size = batch_size))]
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		let mut all_entries: BTreeMap<EncodedKey, Option<CowVec<u8>>> = BTreeMap::new();
		let (start, end) = make_range_bounds(&range);

		if let Some(buffer) = &self.buffer {
			Self::drain_buffer_range(buffer, &start, &end, &mut all_entries)?;
		}
		if let Some(persistent) = &self.persistent {
			Self::drain_persistent_range(persistent, &start, &end, &mut all_entries)?;
		}

		Ok(Self::materialize_batch(all_entries, batch_size))
	}
}

impl StandardSingleStore {
	#[inline]
	fn drain_buffer_range(
		buffer: &SingleBufferTier,
		start: &Bound<Vec<u8>>,
		end: &Bound<Vec<u8>>,
		all_entries: &mut BTreeMap<EncodedKey, Option<CowVec<u8>>>,
	) -> Result<()> {
		let mut cursor = RangeCursor::new();
		loop {
			let batch = buffer.range_next(&mut cursor, bound_as_ref(start), bound_as_ref(end), 4096)?;
			for entry in batch.entries {
				all_entries.entry(entry.key).or_insert(entry.value);
			}
			if cursor.exhausted {
				break;
			}
		}
		Ok(())
	}

	#[inline]
	fn drain_persistent_range(
		persistent: &SinglePersistentTier,
		start: &Bound<Vec<u8>>,
		end: &Bound<Vec<u8>>,
		all_entries: &mut BTreeMap<EncodedKey, Option<CowVec<u8>>>,
	) -> Result<()> {
		let mut cursor = RangeCursor::new();
		loop {
			let batch = persistent.range_next(&mut cursor, bound_as_ref(start), bound_as_ref(end), 4096)?;
			for entry in batch.entries {
				all_entries.entry(entry.key).or_insert(entry.value);
			}
			if cursor.exhausted {
				break;
			}
		}
		Ok(())
	}

	#[inline]
	fn materialize_batch(
		all_entries: BTreeMap<EncodedKey, Option<CowVec<u8>>>,
		batch_size: u64,
	) -> SingleVersionBatch {
		let items: Vec<SingleVersionRow> = all_entries
			.into_iter()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionRow {
					key: EncodedKey::new(key_bytes.to_vec()),
					row: EncodedRow(val),
				})
			})
			.take(batch_size as usize)
			.collect();

		reifydb_assertions! {
			let count = items.len();
			let cap = batch_size as usize;
			assert!(
				count <= cap,
				"range materialize yielded more rows than the requested batch_size, so the has_more paging flag (items.len() >= batch_size) loses meaning and a consumer sizing buffers to batch_size overflows (items={count} batch_size={cap})"
			);
		}

		let has_more = items.len() >= batch_size as usize;

		SingleVersionBatch {
			items,
			has_more,
		}
	}
}

impl SingleVersionRangeRev for StandardSingleStore {
	#[instrument(name = "store::single::range_rev_batch", level = "debug", skip(self), fields(batch_size = batch_size))]
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		let mut all_entries: BTreeMap<EncodedKey, Option<CowVec<u8>>> = BTreeMap::new();
		let (start, end) = make_range_bounds(&range);

		if let Some(buffer) = &self.buffer {
			Self::drain_buffer_range_rev(buffer, &start, &end, &mut all_entries)?;
		}
		if let Some(persistent) = &self.persistent {
			Self::drain_persistent_range_rev(persistent, &start, &end, &mut all_entries)?;
		}

		Ok(Self::materialize_batch_rev(all_entries, batch_size))
	}
}

impl StandardSingleStore {
	#[inline]
	fn drain_buffer_range_rev(
		buffer: &SingleBufferTier,
		start: &Bound<Vec<u8>>,
		end: &Bound<Vec<u8>>,
		all_entries: &mut BTreeMap<EncodedKey, Option<CowVec<u8>>>,
	) -> Result<()> {
		let mut cursor = RangeCursor::new();
		loop {
			let batch = buffer.range_rev_next(&mut cursor, bound_as_ref(start), bound_as_ref(end), 4096)?;
			for entry in batch.entries {
				all_entries.entry(entry.key).or_insert(entry.value);
			}
			if cursor.exhausted {
				break;
			}
		}
		Ok(())
	}

	#[inline]
	fn drain_persistent_range_rev(
		persistent: &SinglePersistentTier,
		start: &Bound<Vec<u8>>,
		end: &Bound<Vec<u8>>,
		all_entries: &mut BTreeMap<EncodedKey, Option<CowVec<u8>>>,
	) -> Result<()> {
		let mut cursor = RangeCursor::new();
		loop {
			let batch =
				persistent.range_rev_next(&mut cursor, bound_as_ref(start), bound_as_ref(end), 4096)?;
			for entry in batch.entries {
				all_entries.entry(entry.key).or_insert(entry.value);
			}
			if cursor.exhausted {
				break;
			}
		}
		Ok(())
	}

	#[inline]
	fn materialize_batch_rev(
		all_entries: BTreeMap<EncodedKey, Option<CowVec<u8>>>,
		batch_size: u64,
	) -> SingleVersionBatch {
		let items: Vec<SingleVersionRow> = all_entries
			.into_iter()
			.rev()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionRow {
					key: EncodedKey::new(key_bytes.to_vec()),
					row: EncodedRow(val),
				})
			})
			.take(batch_size as usize)
			.collect();

		reifydb_assertions! {
			let count = items.len();
			let cap = batch_size as usize;
			assert!(
				count <= cap,
				"reverse range materialize yielded more rows than the requested batch_size, so the has_more paging flag (items.len() >= batch_size) loses meaning and a consumer sizing buffers to batch_size overflows (items={count} batch_size={cap})"
			);
		}

		let has_more = items.len() >= batch_size as usize;

		SingleVersionBatch {
			items,
			has_more,
		}
	}
}

impl SingleVersionStore for StandardSingleStore {}

fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn make_range_bounds(range: &EncodedKeyRange) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
	let start = match &range.start {
		Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
		Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	};

	let end = match &range.end {
		Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
		Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	};

	(start, end)
}
