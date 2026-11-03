// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ops::{Bound, Deref},
	sync::{
		Arc,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{delta::Delta, interface::store::SingleVersionRow, metrics::collect::MetricsCollector};
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorSpawner, ActorSystem},
	},
	context::clock::Clock,
	shutdown::Shutdown,
	sync::mutex::Mutex,
};
use reifydb_value::{count::Count, reifydb_assertions, util::cowvec::CowVec};
use reifydb_wal::lsn::Lsn;
use tracing::instrument;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::flush::actor::{FlushActor, Flusher};
use crate::{
	Result, SingleVersionBatch, SingleVersionCommit, SingleVersionGet, SingleVersionRange, SingleVersionRangeRev,
	SingleVersionStore,
	config::{CommitBufferConfig, SingleStoreConfig},
	flush::actor::FlushMessage,
	tier::{
		RangeBatch, RangeCursor, TierStorage,
		commit::buffer::{SingleCommitBufferTier, SingleCommitMetrics},
		persistent::{SinglePageCacheMetrics, SinglePersistentTier},
	},
};

pub type DirtyMap = HashMap<EncodedKey, Option<CowVec<u8>>>;

const MIN_SCAN_PAGE: u64 = 64;
const MAX_SCAN_PAGE: u64 = 4096;

#[derive(Clone, Copy)]
enum Direction {
	Forward,
	Reverse,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SinglePersistentProbeMetrics {
	pub persistent_probes: Count,
	pub persistent_absent: Count,
}

#[derive(Clone)]
pub struct StandardSingleStore(Arc<StandardSingleStoreInner>);

pub struct SingleCommitGuard(Arc<AtomicUsize>);

impl Drop for SingleCommitGuard {
	fn drop(&mut self) {
		self.0.fetch_sub(1, Ordering::SeqCst);
	}
}

pub struct StandardSingleStoreInner {
	pub(crate) commit: Option<SingleCommitBufferTier>,
	pub(crate) persistent: Option<SinglePersistentTier>,
	#[allow(dead_code)]
	pub(crate) flush_actor: Option<ActorRef<FlushMessage>>,
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub(crate) flusher: Option<Arc<Flusher>>,
	pub(crate) dirty: Arc<Mutex<DirtyMap>>,
	pub(crate) through: Arc<Mutex<Option<Lsn>>>,
	pub(crate) in_flight: Arc<AtomicUsize>,
	pub(crate) persistent_probes: AtomicU64,
	pub(crate) persistent_absent: AtomicU64,
	pub(crate) tombstone_evictions: Arc<AtomicU64>,
	_spawner: ActorSpawner,
}

impl StandardSingleStore {
	#[instrument(name = "store::single::new", level = "debug", skip(config), fields(
		has_buffer = config.commit.is_some(),
		has_persistent = config.persistent.is_some(),
	))]
	pub fn new(config: SingleStoreConfig) -> Result<Self> {
		let commit = config.commit.map(|c| c.storage);
		let spawner = config.spawner.clone();
		let dirty: Arc<Mutex<DirtyMap>> = Arc::new(Mutex::new(HashMap::new()));
		let through: Arc<Mutex<Option<Lsn>>> = Arc::new(Mutex::new(None));
		let in_flight = Arc::new(AtomicUsize::new(0));
		let tombstone_evictions = Arc::new(AtomicU64::new(0));

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush_actor, flusher) = {
			let persistent_cfg = config.persistent.clone();
			let persistent = persistent_cfg.as_ref().map(|c| c.storage.clone());
			let flusher = match (persistent.as_ref(), persistent_cfg.as_ref()) {
				(Some(p), Some(cfg)) => Some((
					Arc::new(Flusher::new(
						Arc::clone(&dirty),
						Arc::clone(&through),
						Arc::clone(&in_flight),
						p.clone(),
						commit.clone(),
						Arc::clone(&tombstone_evictions),
						Arc::clone(&cfg.checkpoint),
					)),
					cfg.flush_interval,
				)),
				_ => None,
			};
			let flush_actor = flusher
				.as_ref()
				.map(|(flusher, interval)| FlushActor::spawn(&spawner, Arc::clone(flusher), *interval));
			(persistent, flush_actor, flusher.map(|(flusher, _)| flusher))
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush_actor): (Option<SinglePersistentTier>, Option<ActorRef<FlushMessage>>) = {
			let _ = config.persistent;
			(None, None)
		};

		Ok(Self(Arc::new(StandardSingleStoreInner {
			commit,
			persistent,
			flush_actor,
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			flusher,
			dirty,
			through,
			in_flight,
			persistent_probes: AtomicU64::new(0),
			persistent_absent: AtomicU64::new(0),
			tombstone_evictions,
			_spawner: spawner,
		})))
	}

	pub fn commit_logged(&self, deltas: CowVec<Delta>, lsn: Lsn) -> Result<()> {
		let entries = self.entries_from_deltas(&deltas);
		self.apply_to_tiers(entries, Some(lsn))
	}

	pub fn begin_commit(&self) -> SingleCommitGuard {
		self.in_flight.fetch_add(1, Ordering::SeqCst);
		SingleCommitGuard(Arc::clone(&self.in_flight))
	}

	pub fn saved_lsn(&self) -> Result<Option<Lsn>> {
		match &self.persistent {
			Some(persistent) => persistent.saved_lsn(),
			None => Ok(None),
		}
	}

	pub fn commit(&self) -> Option<&SingleCommitBufferTier> {
		self.commit.as_ref()
	}

	pub fn persistent(&self) -> Option<&SinglePersistentTier> {
		self.persistent.as_ref()
	}

	pub fn commit_metrics(&self) -> Option<SingleCommitMetrics> {
		self.commit.as_ref().map(SingleCommitBufferTier::metrics)
	}

	pub fn persistent_page_cache_metrics(&self) -> Option<SinglePageCacheMetrics> {
		self.persistent.as_ref().map(SinglePersistentTier::page_cache_metrics)
	}

	pub fn persistent_probe_metrics(&self) -> Option<SinglePersistentProbeMetrics> {
		self.persistent.as_ref().map(|_| SinglePersistentProbeMetrics {
			persistent_probes: Count::new(self.persistent_probes.load(Ordering::Relaxed)),
			persistent_absent: Count::new(self.persistent_absent.load(Ordering::Relaxed)),
		})
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		let mut reporters: Vec<Arc<dyn MetricsCollector>> = Vec::new();
		if let Some(buffer) = self.commit.as_ref() {
			reporters.push(Arc::new(buffer.clone()));
		}
		reporters
	}

	pub fn flush_pending_blocking(&self) -> bool {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(flusher) = self.flusher.as_ref() {
			flusher.drain();
			return true;
		}
		self.persistent.is_none()
	}
}

impl Deref for StandardSingleStore {
	type Target = StandardSingleStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Shutdown for StandardSingleStore {
	fn shutdown(&self) -> Result<()> {
		match self.persistent.as_ref() {
			Some(persistent) => persistent.shutdown(),
			None => Ok(()),
		}
	}
}

impl StandardSingleStore {
	pub fn testing_memory() -> Self {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		Self::new(SingleStoreConfig {
			commit: Some(CommitBufferConfig {
				storage: SingleCommitBufferTier::memory(),
			}),
			persistent: None,
			spawner,
			clock,
		})
		.unwrap()
	}
}

impl SingleVersionGet for StandardSingleStore {
	#[instrument(name = "store::single::get", level = "trace", skip(self, keys), fields(keys = keys.len()))]
	fn get(&self, keys: &[EncodedKey]) -> Result<Vec<Option<SingleVersionRow>>> {
		let mut rows = Vec::with_capacity(keys.len());
		let mut misses = Vec::new();
		let mut buffered = match &self.commit {
			Some(buffer) => {
				let key_slices: Vec<&[u8]> = keys.iter().map(|key| key.as_ref()).collect();
				buffer.get_with_tombstone(&key_slices)?
			}
			None => vec![None; keys.len()],
		};
		for (index, key) in keys.iter().enumerate() {
			rows.push(match buffered[index].take() {
				Some(value) => value.map(|value| SingleVersionRow {
					key: key.clone(),
					bytes: EncodedBytes(value),
				}),
				None => {
					misses.push(index);
					None
				}
			});
		}

		if let Some(persistent) = &self.persistent
			&& !misses.is_empty()
		{
			let probed: Vec<&[u8]> = misses.iter().map(|&index| keys[index].as_ref()).collect();
			self.persistent_probes.fetch_add(misses.len() as u64, Ordering::Relaxed);
			for (index, value) in misses.into_iter().zip(persistent.get(&probed)?) {
				match value {
					Some(value) => {
						rows[index] = Some(SingleVersionRow {
							key: keys[index].clone(),
							bytes: EncodedBytes(value),
						});
					}
					None => {
						self.persistent_absent.fetch_add(1, Ordering::Relaxed);
					}
				}
			}
		}

		Ok(rows)
	}
}

impl SingleVersionCommit for StandardSingleStore {
	#[instrument(name = "store::single::commit", level = "debug", skip(self, deltas), fields(delta_count = deltas.len()))]
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		let entries = self.entries_from_deltas(&deltas);
		self.apply_to_tiers(entries, None)
	}
}

impl StandardSingleStore {
	#[inline]
	fn entries_from_deltas(&self, deltas: &CowVec<Delta>) -> Vec<(EncodedKey, Option<CowVec<u8>>)> {
		deltas.iter()
			.map(|delta| match delta {
				Delta::Set {
					key,
					bytes,
					..
				} => (key.encode(), Some(CowVec::new(bytes.as_ref().to_vec()))),
				Delta::Remove {
					key,
					..
				} => (key.encode(), None),
			})
			.collect()
	}

	#[inline]
	fn apply_to_tiers(&self, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>, lsn: Option<Lsn>) -> Result<()> {
		if let Some(buffer) = &self.commit {
			if self.persistent.is_none() {
				return buffer.set_without_tombstones(entries);
			}
			let mut dirty = self.dirty.lock();
			buffer.set(entries.clone())?;
			for (key, value) in entries {
				dirty.insert(key, value);
			}
			if lsn.is_some() {
				*self.through.lock() = lsn;
			}
		}

		Ok(())
	}
}

impl SingleVersionRange for StandardSingleStore {
	#[instrument(name = "store::single::range_batch", level = "trace", skip(self), fields(batch_size = batch_size))]
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		Ok(Self::materialize_batch(self.merge_tiers(&range, batch_size, Direction::Forward)?, batch_size))
	}
}

impl StandardSingleStore {
	fn merge_tiers(
		&self,
		range: &EncodedKeyRange,
		batch_size: u64,
		direction: Direction,
	) -> Result<BTreeMap<EncodedKey, Option<CowVec<u8>>>> {
		loop {
			let evictions = self.tombstone_evictions.load(Ordering::SeqCst);
			let entries = self.merge_tiers_once(range, batch_size, direction)?;
			if self.tombstone_evictions.load(Ordering::SeqCst) == evictions {
				return Ok(entries);
			}
		}
	}

	fn merge_tiers_once(
		&self,
		range: &EncodedKeyRange,
		batch_size: u64,
		direction: Direction,
	) -> Result<BTreeMap<EncodedKey, Option<CowVec<u8>>>> {
		let (start, end) = make_range_bounds(range);
		let mut entries = BTreeMap::new();
		let mut buffer_cursor = RangeCursor::new();
		let mut persistent_cursor = RangeCursor::new();
		let mut page = batch_size.clamp(MIN_SCAN_PAGE, MAX_SCAN_PAGE) as usize;

		loop {
			match &self.commit {
				Some(buffer) => {
					let batch =
						read_page(buffer, &mut buffer_cursor, &start, &end, page, direction)?;
					for entry in batch.entries {
						entries.insert(entry.key, entry.value);
					}
				}
				None => buffer_cursor.finish(),
			}
			match &self.persistent {
				Some(persistent) => {
					let batch = read_page(
						persistent,
						&mut persistent_cursor,
						&start,
						&end,
						page,
						direction,
					)?;
					for entry in batch.entries {
						entries.entry(entry.key).or_insert(entry.value);
					}
				}
				None => persistent_cursor.finish(),
			}

			if buffer_cursor.is_exhausted() && persistent_cursor.is_exhausted() {
				return Ok(entries);
			}
			if settled_live(&entries, &buffer_cursor, &persistent_cursor, batch_size, direction)
				>= batch_size
			{
				return Ok(entries);
			}
			page = (page * 2).min(MAX_SCAN_PAGE as usize);
		}
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
					key: EncodedKey::new(key_bytes),
					bytes: EncodedBytes(val),
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
	#[instrument(name = "store::single::range_rev_batch", level = "trace", skip(self), fields(batch_size = batch_size))]
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		Ok(Self::materialize_batch_rev(self.merge_tiers(&range, batch_size, Direction::Reverse)?, batch_size))
	}
}

impl StandardSingleStore {
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
					key: EncodedKey::new(key_bytes),
					bytes: EncodedBytes(val),
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

fn read_page<T: TierStorage>(
	tier: &T,
	cursor: &mut RangeCursor,
	start: &Bound<Vec<u8>>,
	end: &Bound<Vec<u8>>,
	page: usize,
	direction: Direction,
) -> Result<RangeBatch> {
	if cursor.is_exhausted() {
		return Ok(RangeBatch::empty());
	}
	match direction {
		Direction::Forward => tier.range_next(cursor, bound_as_ref(start), bound_as_ref(end), page),
		Direction::Reverse => tier.range_rev_next(cursor, bound_as_ref(start), bound_as_ref(end), page),
	}
}

fn settled_live(
	entries: &BTreeMap<EncodedKey, Option<CowVec<u8>>>,
	buffer_cursor: &RangeCursor,
	persistent_cursor: &RangeCursor,
	batch_size: u64,
	direction: Direction,
) -> u64 {
	let frontiers = [buffer_cursor, persistent_cursor]
		.into_iter()
		.filter(|cursor| !cursor.is_exhausted())
		.filter_map(|cursor| cursor.last_key());
	let wanted = batch_size as usize;
	let live = match direction {
		Direction::Forward => match frontiers.min() {
			Some(frontier) => entries
				.range::<EncodedKey, _>((Bound::Unbounded, Bound::Included(frontier)))
				.filter(|(_, value)| value.is_some())
				.take(wanted)
				.count(),
			None => entries.values().filter(|value| value.is_some()).take(wanted).count(),
		},
		Direction::Reverse => match frontiers.max() {
			Some(frontier) => entries
				.range::<EncodedKey, _>((Bound::Included(frontier), Bound::Unbounded))
				.rev()
				.filter(|(_, value)| value.is_some())
				.take(wanted)
				.count(),
			None => entries.values().rev().filter(|value| value.is_some()).take(wanted).count(),
		},
	};
	live as u64
}

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

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod tests {
	use std::{panic, thread};

	use reifydb_core::{
		delta::{RemoveAnnounce, SetPre},
		interface::{
			catalog::id::QueueId,
			store::{SingleVersionCommit, WalFloorCheckpoint},
		},
		key::{any::TaggedKey, queue::QueueDeduplicationKey},
	};
	use reifydb_sqlite::SqliteTempPathGuard;

	use super::*;
	use crate::config::PersistentConfig;

	struct NoWal;

	impl WalFloorCheckpoint for NoWal {
		fn make_durable(&self, _through: Lsn) -> Result<()> {
			Ok(())
		}

		fn saved_through(&self, _through: Lsn) -> Result<()> {
			Ok(())
		}

		fn durable_head(&self) -> Result<Option<Lsn>> {
			Ok(None)
		}

		fn appended_head(&self) -> Result<Option<Lsn>> {
			panic!("not used by single")
		}
	}

	fn memory_with_persistent_sqlite(
		checkpoint: Arc<dyn WalFloorCheckpoint>,
	) -> (StandardSingleStore, SqliteTempPathGuard) {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		let (persistent, guard) = PersistentConfig::sqlite_in_memory(checkpoint);
		let store = StandardSingleStore::new(SingleStoreConfig {
			commit: Some(CommitBufferConfig {
				storage: SingleCommitBufferTier::memory(),
			}),
			persistent: Some(persistent),
			spawner,
			clock,
		})
		.unwrap();
		(store, guard)
	}

	fn key(name: &str) -> TaggedKey {
		QueueDeduplicationKey::new(QueueId(1), name.as_bytes().iter().map(|b| !b).collect::<Vec<u8>>()).into()
	}

	#[test]
	fn a_flush_that_cannot_persist_stops_with_a_named_panic() {
		// A failed persist must stop the caller loudly, otherwise the rows live only in memory and vanish on
		// restart.
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));

		let k = key("counter");
		SingleVersionCommit::commit(
			&mut store,
			CowVec::new(vec![Delta::Set {
				key: k.clone(),
				bytes: EncodedBytes(CowVec::new(b"7".to_vec())),
				pre: SetPre::Unknown,
			}]),
		)
		.unwrap();
		assert!(store.dirty.lock().contains_key(&k.encode()), "the write is pending a flush");

		store.persistent().expect("persistent tier configured").shutdown().unwrap();

		let payload = panic::catch_unwind(panic::AssertUnwindSafe(|| store.flush_pending_blocking()))
			.expect_err("a flush that cannot persist must never report the batch durable");
		let message = payload
			.downcast_ref::<String>()
			.cloned()
			.or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
			.unwrap_or_default();
		assert!(
			message.starts_with("single flush could not persist 1 rows"),
			"a failed persist must stop the flush with a named panic, otherwise the rows live only in memory \
			 and the counters behind them are gone after a restart; panic was: {message:?}"
		);
	}

	struct RecordingWal {
		calls: Mutex<Vec<(&'static str, u64)>>,
	}

	impl WalFloorCheckpoint for RecordingWal {
		fn make_durable(&self, through: Lsn) -> Result<()> {
			self.calls.lock().push(("make_durable", through.as_u64()));
			Ok(())
		}

		fn saved_through(&self, through: Lsn) -> Result<()> {
			self.calls.lock().push(("saved_through", through.as_u64()));
			Ok(())
		}

		fn durable_head(&self) -> Result<Option<Lsn>> {
			panic!("recording wal has no durable head; this fake must not reach the idle pass")
		}

		fn appended_head(&self) -> Result<Option<Lsn>> {
			panic!("not used by single")
		}
	}

	#[test]
	fn a_flush_that_cannot_persist_never_moves_the_commit_log_floor() {
		// A floor moved before sqlite holds the rows lets gc drop the only durable copy, so a crash loses them.
		let wal = Arc::new(RecordingWal {
			calls: Mutex::new(Vec::new()),
		});
		let (store, _guard) = memory_with_persistent_sqlite(Arc::clone(&wal) as Arc<dyn WalFloorCheckpoint>);

		store.commit_logged(
			CowVec::new(vec![Delta::Set {
				key: key("counter"),
				bytes: EncodedBytes(CowVec::new(b"7".to_vec())),
				pre: SetPre::Unknown,
			}]),
			Lsn::new(4),
		)
		.unwrap();

		store.persistent().expect("persistent tier configured").shutdown().unwrap();

		panic::catch_unwind(panic::AssertUnwindSafe(|| store.flush_pending_blocking()))
			.expect_err("a flush that cannot persist must stop");
		assert_eq!(
			*wal.calls.lock(),
			vec![("make_durable", 4)],
			"the log must be made durable but its floor must stay put while sqlite has not saved lsn 4"
		);
	}

	struct DurableHeadWal {
		durable_head: Lsn,
		racer: Mutex<Option<StandardSingleStore>>,
		racing: Mutex<Vec<SingleCommitGuard>>,
		saved: Mutex<Vec<u64>>,
	}

	impl DurableHeadWal {
		fn at(head: u64) -> Arc<Self> {
			Arc::new(Self {
				durable_head: Lsn::new(head),
				racer: Mutex::new(None),
				racing: Mutex::new(Vec::new()),
				saved: Mutex::new(Vec::new()),
			})
		}
	}

	impl WalFloorCheckpoint for DurableHeadWal {
		fn make_durable(&self, _through: Lsn) -> Result<()> {
			Ok(())
		}

		fn saved_through(&self, through: Lsn) -> Result<()> {
			self.saved.lock().push(through.as_u64());
			Ok(())
		}

		fn durable_head(&self) -> Result<Option<Lsn>> {
			match self.racer.lock().as_ref() {
				Some(store) => {
					self.racing.lock().push(store.begin_commit());
					Ok(Some(Lsn::new(self.durable_head.as_u64() + 1)))
				}
				None => Ok(Some(self.durable_head)),
			}
		}

		fn appended_head(&self) -> Result<Option<Lsn>> {
			panic!("not used by single")
		}
	}

	#[test]
	fn a_held_commit_guard_keeps_a_quiet_flush_from_advancing() {
		// A floor moved past a commit still between its log append and the dirty map lets gc drop that record.
		let wal = DurableHeadWal::at(7);
		let (store, _guard) = memory_with_persistent_sqlite(Arc::clone(&wal) as Arc<dyn WalFloorCheckpoint>);

		let _held = store.begin_commit();
		assert!(store.flush_pending_blocking());

		assert!(
			wal.saved.lock().is_empty(),
			"a quiet flush must not move the single floor while a commit is in flight, otherwise gc drops a \
			 record the store never saved"
		);
	}

	#[test]
	fn the_durable_head_is_read_before_the_in_flight_check() {
		// A head read after the in-flight check can cover a commit that started in between, which must never be
		// saved.
		let wal = DurableHeadWal::at(7);
		let (store, _guard) = memory_with_persistent_sqlite(Arc::clone(&wal) as Arc<dyn WalFloorCheckpoint>);
		*wal.racer.lock() = Some(store.clone());

		assert!(store.flush_pending_blocking());

		assert!(
			wal.saved.lock().is_empty(),
			"the commit that started while the head was read must keep the floor put, otherwise the floor \
			 passes lsn 8 before the store holds it"
		);
	}

	#[test]
	fn a_dropped_guard_lets_the_next_quiet_flush_advance() {
		// A guard that never releases its count pins the single floor forever, so gc never drops a segment
		// again.
		let wal = DurableHeadWal::at(7);
		let (store, _guard) = memory_with_persistent_sqlite(Arc::clone(&wal) as Arc<dyn WalFloorCheckpoint>);

		let held = store.begin_commit();
		assert!(store.flush_pending_blocking());
		assert!(wal.saved.lock().is_empty(), "the held guard must keep the floor put");

		drop(held);
		assert!(store.flush_pending_blocking());

		assert_eq!(
			*wal.saved.lock(),
			vec![7],
			"once the commit finished, a quiet flush must move the single floor to the log head"
		);
	}

	#[test]
	fn flush_pending_blocking_reports_whether_the_batch_became_durable() {
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));

		let k = key("intern");
		SingleVersionCommit::commit(
			&mut store,
			CowVec::new(vec![Delta::Set {
				key: k.clone(),
				bytes: EncodedBytes(CowVec::new(b"1".to_vec())),
				pre: SetPre::Unknown,
			}]),
		)
		.unwrap();
		assert!(store.flush_pending_blocking(), "a healthy persistent tier must report a completed flush");
		assert!(store.dirty.lock().is_empty(), "the reported success must mean the batch left the dirty map");
	}

	fn probes(store: &StandardSingleStore) -> (u64, u64) {
		let m = store.persistent_probe_metrics().expect("persistent tier configured");
		(m.persistent_probes.as_u64(), m.persistent_absent.as_u64())
	}

	fn seed_persistent(store: &StandardSingleStore, k: &TaggedKey, value: &str) {
		store.persistent()
			.expect("persistent tier configured")
			.set(vec![(k.encode(), Some(CowVec::new(value.as_bytes().to_vec())))])
			.unwrap();
	}

	#[test]
	fn a_read_the_commit_buffer_answers_never_counts_a_persistent_probe() {
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));

		let present = key("buffered");
		SingleVersionCommit::commit(
			&mut store,
			CowVec::new(vec![Delta::Set {
				key: present.clone(),
				bytes: EncodedBytes(CowVec::new(b"7".to_vec())),
				pre: SetPre::Unknown,
			}]),
		)
		.unwrap();
		let removed = key("buffered-tombstone");
		SingleVersionCommit::commit(&mut store, CowVec::new(vec![Delta::remove_silent(removed.clone())]))
			.unwrap();

		let before = probes(&store);
		assert!(SingleVersionGet::get(&store, &[present.encode()]).unwrap().pop().flatten().is_some());
		assert!(SingleVersionGet::get(&store, &[removed.encode()]).unwrap().pop().flatten().is_none());

		assert_eq!(
			probes(&store),
			before,
			"the commit buffer answered every one of these reads, including the tombstone. Counting 			 them as persistent probes inflates the denominator with lookups no filter could ever 			 have skipped, so the measured absent rate reads lower than the real ceiling"
		);
	}

	#[test]
	fn a_persistent_read_that_finds_a_row_counts_a_probe_but_no_absence() {
		let (store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));

		let k = key("resident");
		seed_persistent(&store, &k, "9");

		let before = probes(&store);
		assert!(SingleVersionGet::get(&store, &[k.encode()]).unwrap().pop().flatten().is_some());
		assert_eq!(
			probes(&store),
			(before.0 + 1, before.1),
			"a hit must raise the probe count and leave the absent count alone, otherwise the ratio 			 claims a filter could skip reads that genuinely returned data"
		);
	}

	#[test]
	fn a_persistent_read_that_finds_nothing_counts_a_probe_and_an_absence() {
		let (store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));

		let k = key("never-written");

		let before = probes(&store);
		assert!(SingleVersionGet::get(&store, &[k.encode()]).unwrap().pop().flatten().is_none());
		assert_eq!(
			probes(&store),
			(before.0 + 1, before.1 + 1),
			"an absent point read must raise both counts, otherwise the ceiling on what a filter 			 could save measures as zero and the decision is made on a number that cannot move"
		);
	}

	#[test]
	fn a_store_without_a_persistent_tier_reports_no_probe_metrics() {
		let store = StandardSingleStore::testing_memory();
		assert!(SingleVersionGet::get(&store, &[key("anything").encode()]).unwrap().pop().flatten().is_none());
		assert!(store.persistent_probe_metrics().is_none());
	}

	fn write_value(store: &mut StandardSingleStore, k: &TaggedKey, value: &[u8]) {
		SingleVersionCommit::commit(
			store,
			CowVec::new(vec![Delta::Set {
				key: k.clone(),
				bytes: EncodedBytes(CowVec::new(value.to_vec())),
				pre: SetPre::Unknown,
			}]),
		)
		.unwrap();
	}

	fn remove_key(store: &mut StandardSingleStore, k: &TaggedKey) {
		SingleVersionCommit::commit(
			store,
			CowVec::new(vec![Delta::Remove {
				key: k.clone(),
				announce: RemoveAnnounce::Silent,
			}]),
		)
		.unwrap();
	}

	fn numbered(index: usize) -> TaggedKey {
		key(&format!("{index:06}"))
	}

	fn assert_scans_match(store: &StandardSingleStore, model: &BTreeMap<EncodedKey, Vec<u8>>, stage: &str) {
		for batch_size in [0u64, 1, 2, 10, 63, 64, 65, 500, 4096, 100_000] {
			let wanted = batch_size as usize;
			let forward =
				SingleVersionRange::range_batch(store, EncodedKeyRange::all(), batch_size).unwrap();
			let expected: Vec<(&EncodedKey, &Vec<u8>)> = model.iter().take(wanted).collect();
			let actual: Vec<(&EncodedKey, Vec<u8>)> =
				forward.items.iter().map(|row| (&row.key, row.bytes.as_ref().to_vec())).collect();
			assert_eq!(actual.len(), expected.len(), "{stage}: forward size at batch {batch_size}");
			for (actual, expected) in actual.iter().zip(&expected) {
				assert_eq!(
					(actual.0, &actual.1),
					*expected,
					"{stage}: forward row at batch {batch_size}"
				);
			}
			assert_eq!(
				forward.has_more,
				expected.len() >= wanted,
				"{stage}: forward has_more at {batch_size}"
			);

			let reverse = SingleVersionRangeRev::range_rev_batch(store, EncodedKeyRange::all(), batch_size)
				.unwrap();
			let expected: Vec<(&EncodedKey, &Vec<u8>)> = model.iter().rev().take(wanted).collect();
			let actual: Vec<(&EncodedKey, Vec<u8>)> =
				reverse.items.iter().map(|row| (&row.key, row.bytes.as_ref().to_vec())).collect();
			assert_eq!(actual.len(), expected.len(), "{stage}: reverse size at batch {batch_size}");
			for (actual, expected) in actual.iter().zip(&expected) {
				assert_eq!(
					(actual.0, &actual.1),
					*expected,
					"{stage}: reverse row at batch {batch_size}"
				);
			}
			assert_eq!(
				reverse.has_more,
				expected.len() >= wanted,
				"{stage}: reverse has_more at {batch_size}"
			);
		}
	}

	#[test]
	fn a_stopped_scan_returns_what_a_full_merge_of_both_tiers_would() {
		// The buffer must beat the persistent tier and tombstones must never count, or claims skip or resurrect
		// items.
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));
		let mut model = BTreeMap::new();
		for index in 0..600 {
			let k = numbered(index);
			write_value(&mut store, &k, b"old");
			model.insert(k.encode(), b"old".to_vec());
		}
		assert!(store.flush_pending_blocking());
		assert_scans_match(&store, &model, "all persisted");

		for index in (0..600).step_by(3) {
			let k = numbered(index);
			remove_key(&mut store, &k);
			model.remove(&k.encode());
		}
		for index in (0..600).step_by(5) {
			let k = numbered(index);
			write_value(&mut store, &k, b"new");
			model.insert(k.encode(), b"new".to_vec());
		}
		for index in 600..650 {
			let k = numbered(index);
			write_value(&mut store, &k, b"fresh");
			model.insert(k.encode(), b"fresh".to_vec());
		}
		assert_scans_match(&store, &model, "unflushed buffer over persisted rows");

		assert!(store.flush_pending_blocking());
		assert_scans_match(&store, &model, "flushed");
	}

	#[test]
	fn a_small_scan_reads_a_bounded_part_of_a_deep_range() {
		// Reading the whole range per call makes every claim cost the depth of its partition.
		let mut memory = StandardSingleStore::testing_memory();
		let (mut tiered, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));
		for index in 0..5_000 {
			write_value(&mut memory, &numbered(index), b"v");
			write_value(&mut tiered, &numbered(index), b"v");
		}
		assert!(tiered.flush_pending_blocking());
		let all = EncodedKeyRange::all();
		for direction in [Direction::Forward, Direction::Reverse] {
			let read = memory.merge_tiers(&all, 10, direction).unwrap().len() as u64;
			assert!(read <= MIN_SCAN_PAGE, "a memory scan for 10 read {read} keys");
			let read = tiered.merge_tiers(&all, 10, direction).unwrap().len() as u64;
			assert!(read <= MIN_SCAN_PAGE * 2, "a tiered scan for 10 read {read} keys");
		}
	}

	#[test]
	fn claimed_keys_do_not_slow_a_later_scan_once_flushed() {
		// Tombstones sit where a claim starts reading, so a kept one makes every later claim walk all past
		// removals.
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));
		for index in 0..5_000 {
			write_value(&mut store, &numbered(index), b"v");
		}
		assert!(store.flush_pending_blocking());
		let all = EncodedKeyRange::all();
		let first = SingleVersionRangeRev::range_rev_batch(&store, all.clone(), 5_000).unwrap();
		for row in &first.items[..4_000] {
			SingleVersionCommit::commit(
				&mut store,
				CowVec::new(vec![Delta::Remove {
					key: TaggedKey::decode(&row.key).unwrap(),
					announce: RemoveAnnounce::Silent,
				}]),
			)
			.unwrap();
		}
		assert!(store.flush_pending_blocking());

		let read = store.merge_tiers(&all, 10, Direction::Reverse).unwrap().len() as u64;
		assert!(read <= MIN_SCAN_PAGE * 2, "a scan after a flush walked {read} keys, so tombstones survived");
		let (resident, _) = store.commit.as_ref().unwrap().memory_usage();
		assert_eq!(resident, 1_000, "flushed tombstones must leave the commit buffer");
	}

	#[test]
	fn a_flush_never_loses_a_delete_or_a_later_write() {
		// Dropping a tombstone before its delete is durable resurrects the row; dropping a rewrite loses it.
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));
		let gone = key("gone");
		let back = key("back");
		write_value(&mut store, &gone, b"1");
		write_value(&mut store, &back, b"1");
		assert!(store.flush_pending_blocking());

		remove_key(&mut store, &gone);
		remove_key(&mut store, &back);
		assert!(store.flush_pending_blocking());
		write_value(&mut store, &back, b"2");

		let read = |store: &StandardSingleStore, k: &TaggedKey| {
			SingleVersionGet::get(store, &[k.encode()]).unwrap().pop().flatten()
		};
		assert!(read(&store, &gone).is_none(), "a flushed delete must stay deleted");
		assert_eq!(read(&store, &back).unwrap().bytes.as_ref(), b"2", "a rewrite after the delete must win");
		assert!(store.flush_pending_blocking());
		assert!(read(&store, &gone).is_none(), "a second flush must not bring the row back");
		assert_eq!(read(&store, &back).unwrap().bytes.as_ref(), b"2", "the rewrite must survive its own flush");
		let persistent = store.persistent.as_ref().unwrap();
		assert_eq!(
			persistent.get_with_tombstone(&[gone.encode().as_ref()]).unwrap().remove(0),
			None,
			"a delete must remove the row, otherwise every scan keeps walking it"
		);
	}

	#[test]
	fn a_memory_store_keeps_no_tombstone() {
		// With no tier underneath there is nothing to hide, so a kept tombstone only grows memory and slows
		// scans.
		let mut store = StandardSingleStore::testing_memory();
		let k = key("once");
		write_value(&mut store, &k, b"1");
		remove_key(&mut store, &k);
		assert_eq!(store.commit.as_ref().unwrap().memory_usage().0, 0);
		assert!(SingleVersionGet::get(&store, &[k.encode()]).unwrap().pop().flatten().is_none());
	}

	#[test]
	fn a_scan_racing_flushes_never_returns_a_key_removed_before_it_began() {
		// A tombstone evicted mid scan must not let an older persisted page bring its removed key back.
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));
		const KEYS: usize = 3_000;
		for index in 0..KEYS {
			write_value(&mut store, &numbered(index), b"v");
		}
		assert!(store.flush_pending_blocking());
		let ordered =
			SingleVersionRangeRev::range_rev_batch(&store, EncodedKeyRange::all(), KEYS as u64).unwrap();
		let keys: Vec<EncodedKey> = ordered.items.iter().map(|row| row.key.clone()).collect();

		let removed = AtomicUsize::new(0);
		thread::scope(|threads| {
			let mut writer = store.clone();
			let (removed, keys) = (&removed, &keys);
			threads.spawn(move || {
				for (index, encoded) in keys.iter().enumerate() {
					remove_key(&mut writer, &TaggedKey::decode(encoded).unwrap());
					removed.store(index + 1, Ordering::SeqCst);
					if index % 7 == 0 {
						assert!(writer.flush_pending_blocking());
					}
				}
			});
			while removed.load(Ordering::SeqCst) < KEYS {
				let floor = removed.load(Ordering::SeqCst);
				let batch = SingleVersionRangeRev::range_rev_batch(&store, EncodedKeyRange::all(), 16)
					.unwrap();
				for row in &batch.items {
					let position = keys.binary_search_by(|key| row.key.cmp(key)).unwrap();
					assert!(
						position >= floor,
						"key {position} came back after {floor} were removed"
					);
				}
			}
		});
	}

	#[test]
	fn a_stopped_scan_finds_rows_that_only_the_persistent_tier_holds() {
		// After a restart the buffer lacks old rows, so stopping on one tier's position would skip keys in the
		// other.
		let (mut store, _guard) = memory_with_persistent_sqlite(Arc::new(NoWal));
		let mut model = BTreeMap::new();
		let persisted: Vec<(EncodedKey, Option<CowVec<u8>>)> = (0..400)
			.map(|index| {
				model.insert(numbered(index).encode(), b"disk".to_vec());
				(numbered(index).encode(), Some(CowVec::new(b"disk".to_vec())))
			})
			.collect();
		store.persistent.as_ref().unwrap().persist(persisted, None).unwrap();
		for index in 200..600 {
			let k = numbered(index);
			write_value(&mut store, &k, b"buffer");
			model.insert(k.encode(), b"buffer".to_vec());
		}
		for index in (0..10).chain(590..600) {
			let k = numbered(index);
			remove_key(&mut store, &k);
			model.remove(&k.encode());
		}
		assert_scans_match(&store, &model, "disk rows under a partial buffer");

		assert!(store.flush_pending_blocking());
		assert_scans_match(&store, &model, "disk rows after a flush");
	}

	#[test]
	fn removing_tombstones_never_drops_a_live_value() {
		// A key rewritten after its delete was flushed holds the only copy of its value in the buffer.
		let buffer = SingleCommitBufferTier::memory();
		let live = key("live").encode();
		let removed = key("removed").encode();
		let absent = key("absent").encode();
		buffer.set(vec![(live.clone(), Some(CowVec::new(b"1".to_vec()))), (removed.clone(), None)]).unwrap();

		buffer.remove_tombstones(&[&live, &removed, &absent]);

		assert_eq!(
			buffer.get_with_tombstone(&[live.as_ref()]).unwrap().remove(0).flatten().unwrap().as_ref(),
			b"1"
		);
		assert_eq!(buffer.get_with_tombstone(&[removed.as_ref()]).unwrap().remove(0), None);
	}
}
