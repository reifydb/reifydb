// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ops::{Bound, Deref},
	sync::{Arc, Mutex},
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
	actor::{mailbox::ActorRef, system::ActorSystem},
	context::clock::Clock,
	pool::{PoolConfig, Pools},
	sync::waiter::WaiterHandle,
};
use reifydb_type::util::{cowvec::CowVec, hex};
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

pub type DirtyMap = HashMap<CowVec<u8>, Option<CowVec<u8>>>;

#[derive(Clone)]
pub struct StandardSingleStore(Arc<StandardSingleStoreInner>);

pub struct StandardSingleStoreInner {
	pub(crate) buffer: Option<SingleBufferTier>,
	pub(crate) persistent: Option<SinglePersistentTier>,
	#[allow(dead_code)]
	pub(crate) flush_actor: Option<ActorRef<FlushMessage>>,
	pub(crate) dirty: Arc<Mutex<DirtyMap>>,
	_actor_system: ActorSystem,
}

impl StandardSingleStore {
	#[instrument(name = "store::single::new", level = "debug", skip(config), fields(
		has_buffer = config.buffer.is_some(),
		has_persistent = config.persistent.is_some(),
	))]
	pub fn new(config: SingleStoreConfig) -> Result<Self> {
		let buffer = config.buffer.map(|c| c.storage);
		let actor_system = config.actor_system.clone();
		let dirty: Arc<Mutex<DirtyMap>> = Arc::new(Mutex::new(HashMap::new()));

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush_actor) = {
			let persistent_cfg = config.persistent.clone();
			let persistent = persistent_cfg.as_ref().map(|c| c.storage.clone());
			let flush_actor = match (persistent.as_ref(), persistent_cfg.as_ref()) {
				(Some(p), Some(cfg)) => Some(FlushActor::spawn(
					&actor_system,
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
			_actor_system: actor_system,
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

		if self.dirty.lock().unwrap().is_empty() {
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

impl StandardSingleStore {
	pub fn testing_memory() -> Self {
		let pools = Pools::new(PoolConfig::sync_only());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: None,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap()
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: Some(PersistentConfig::sqlite_in_memory()),
			actor_system,
			clock: Clock::Real,
		})
		.unwrap()
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
		let entries: Vec<(CowVec<u8>, Option<CowVec<u8>>)> = deltas
			.iter()
			.map(|delta| match delta {
				Delta::Set {
					key,
					row,
				} => (CowVec::new(key.as_ref().to_vec()), Some(CowVec::new(row.as_ref().to_vec()))),
				Delta::Unset {
					key,
					..
				}
				| Delta::Remove {
					key,
				}
				| Delta::Drop {
					key,
				} => (CowVec::new(key.as_ref().to_vec()), None),
			})
			.collect();

		if let Some(buffer) = &self.buffer {
			buffer.set(entries.clone())?;
			if self.persistent.is_some() {
				let mut dirty = self.dirty.lock().unwrap();
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
		let mut all_entries: BTreeMap<CowVec<u8>, Option<CowVec<u8>>> = BTreeMap::new();

		let (start, end) = make_range_bounds(&range);

		if let Some(buffer) = &self.buffer {
			let mut cursor = RangeCursor::new();

			loop {
				let batch =
					buffer.range_next(&mut cursor, bound_as_ref(&start), bound_as_ref(&end), 4096)?;

				for entry in batch.entries {
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}
		}

		if let Some(persistent) = &self.persistent {
			let mut cursor = RangeCursor::new();

			loop {
				let batch = persistent.range_next(
					&mut cursor,
					bound_as_ref(&start),
					bound_as_ref(&end),
					4096,
				)?;

				for entry in batch.entries {
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}
		}

		let items: Vec<SingleVersionRow> = all_entries
			.into_iter()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionRow {
					key: EncodedKey(key_bytes),
					row: EncodedRow(val),
				})
			})
			.take(batch_size as usize)
			.collect();

		let has_more = items.len() >= batch_size as usize;

		Ok(SingleVersionBatch {
			items,
			has_more,
		})
	}
}

impl SingleVersionRangeRev for StandardSingleStore {
	#[instrument(name = "store::single::range_rev_batch", level = "debug", skip(self), fields(batch_size = batch_size))]
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		let mut all_entries: BTreeMap<CowVec<u8>, Option<CowVec<u8>>> = BTreeMap::new();

		let (start, end) = make_range_bounds(&range);

		if let Some(buffer) = &self.buffer {
			let mut cursor = RangeCursor::new();

			loop {
				let batch = buffer.range_rev_next(
					&mut cursor,
					bound_as_ref(&start),
					bound_as_ref(&end),
					4096,
				)?;

				for entry in batch.entries {
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}
		}

		if let Some(persistent) = &self.persistent {
			let mut cursor = RangeCursor::new();

			loop {
				let batch = persistent.range_rev_next(
					&mut cursor,
					bound_as_ref(&start),
					bound_as_ref(&end),
					4096,
				)?;

				for entry in batch.entries {
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}
		}

		let items: Vec<SingleVersionRow> = all_entries
			.into_iter()
			.rev()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionRow {
					key: EncodedKey(key_bytes),
					row: EncodedRow(val),
				})
			})
			.take(batch_size as usize)
			.collect();

		let has_more = items.len() >= batch_size as usize;

		Ok(SingleVersionBatch {
			items,
			has_more,
		})
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
