// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_codec::key::encoded::EncodedKey;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::event::metric::{MultiEviction, MultiPersist, MultiSweptEvent};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::lifecycle::progress::Progress;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_core::{event::EventBus, lifecycle::watermark::EvictionWatermark};
use reifydb_runtime::{
	context::clock::Clock,
	sync::{mutex::Mutex, rwlock::RwLock},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_value::byte_size::ByteSize;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_value::{reifydb_assertions, util::cowvec::CowVec};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use tracing::{debug, error, warn};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::TierBatch;
#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::TierStorage;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::commit::memory::storage::EvictedVersion;
use crate::{
	flush::ObjectPersistence,
	tier::{commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier, read::MultiReadBufferTier},
};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub const FLUSH_KEY_BUDGET: usize = 2048;

#[derive(Default)]
pub struct FlushEngineState {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	resume_from: Option<EntryKind>,
}

#[allow(dead_code)]
pub struct FlushEngine {
	commit: MultiCommitBufferTier,
	persistent: MultiPersistentTier,
	persistence: Arc<OnceLock<Arc<dyn ObjectPersistence>>>,
	eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,
	read: Option<MultiReadBufferTier>,
	clock: Clock,
	event_bus: EventBus,
	sweep_lock: Mutex<FlushEngineState>,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
type EvictablePersist = Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
type EvictableDrop = Vec<EvictedVersion>;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
type EvictablePartition = (EvictablePersist, EvictableDrop);

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub struct SweepOutcome {
	pub progress: Progress,
	pub reclaimed: u64,
	pub backlog: u64,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl FlushEngine {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		commit: MultiCommitBufferTier,
		persistent: MultiPersistentTier,
		persistence: Arc<OnceLock<Arc<dyn ObjectPersistence>>>,
		eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,
		read: Option<MultiReadBufferTier>,
		clock: Clock,
		event_bus: EventBus,
	) -> Self {
		Self {
			commit,
			persistent,
			persistence,
			eviction_watermark,
			read,
			clock,
			event_bus,
			sweep_lock: Mutex::new(FlushEngineState::default()),
		}
	}

	pub fn sweep_slice(&self, budget: usize) -> SweepOutcome {
		let mut state = self.sweep_lock.lock();
		let (progress, reclaimed) = match self.eviction_cutoff() {
			Some(cutoff) => self.sweep_once(&mut state, cutoff, budget),
			None => (Progress::Exhausted, 0),
		};
		SweepOutcome {
			progress,
			reclaimed,
			backlog: self.buffered_entries(),
		}
	}

	fn buffered_entries(&self) -> u64 {
		self.commit
			.list_all_entry_kinds()
			.map(|kinds| kinds.iter().map(|kind| self.commit.count_current(*kind).unwrap_or(0)).sum())
			.unwrap_or(0)
	}

	pub fn flush_pending(&self) {
		let mut guard = self.sweep_lock.lock();
		if let Some(cutoff) = self.eviction_cutoff() {
			while self.sweep_once(&mut guard, cutoff, FLUSH_KEY_BUDGET).0.is_yielded() {}
		}
	}

	pub fn flush_all(&self) {
		let mut guard = self.sweep_lock.lock();
		while self.sweep_once(&mut guard, CommitVersion(u64::MAX), FLUSH_KEY_BUDGET).0.is_yielded() {}
	}

	fn eviction_cutoff(&self) -> Option<CommitVersion> {
		let cutoff = self.eviction_watermark.read().as_ref()?.watermark();
		if cutoff.0 == 0 {
			return None;
		}
		Some(cutoff)
	}

	fn is_persistent_object(&self, kind: EntryKind) -> bool {
		match kind {
			EntryKind::Source(storage) | EntryKind::PartitionedSource(storage) => {
				self.persistence.get().map(|provider| provider.is_persistent(storage)).unwrap_or(true)
			}
			EntryKind::Multi => true,
		}
	}

	#[cfg(test)]
	fn sweep(&self, cutoff: CommitVersion) {
		let mut guard = self.sweep_lock.lock();
		while self.sweep_once(&mut guard, cutoff, FLUSH_KEY_BUDGET).0.is_yielded() {}
	}

	fn sweep_once(&self, state: &mut FlushEngineState, cutoff: CommitVersion, budget: usize) -> (Progress, u64) {
		let Some(mut entry_kinds) = self.list_evictable_kinds() else {
			return (Progress::Exhausted, 0);
		};
		if let Some(resume) = state.resume_from
			&& let Some(position) = entry_kinds.iter().position(|kind| *kind == resume)
		{
			entry_kinds.rotate_left(position);
		}
		state.resume_from = None;

		let mut remaining = budget;
		let mut more = false;
		let mut plan: Vec<(EntryKind, bool, EvictablePartition)> = Vec::new();
		let mut batches: HashMap<CommitVersion, TierBatch> = HashMap::new();
		for kind in entry_kinds {
			if remaining == 0 {
				more = true;
				state.resume_from = Some(kind);
				break;
			}
			let (to_persist, to_drop, kind_more) = self.collect_evictable(kind, cutoff, remaining);
			if to_persist.is_empty() && to_drop.is_empty() {
				continue;
			}
			remaining = remaining.saturating_sub(to_persist.len());
			more |= kind_more;
			let persistent_object = self.is_persistent_object(kind);
			if persistent_object {
				for (key, version, value) in &to_persist {
					batches.entry(*version)
						.or_default()
						.entry(kind)
						.or_default()
						.push((key.clone(), value.clone()));
				}
			}
			plan.push((kind, persistent_object, (to_persist, to_drop)));
		}
		if plan.is_empty() {
			return (Progress::Exhausted, 0);
		}

		let accepted = if batches.values().any(|batch| !batch.is_empty()) {
			match self.persistent.persist_sweep(batches.into_iter().collect()) {
				Ok(accepted) => accepted,
				Err(e) => {
					error!(error = %e, "flush sweep: persist failed, aborting slice");
					return (Progress::Exhausted, 0);
				}
			}
		} else {
			Vec::new()
		};
		let persisted = accepted.len();

		let accepted_keys: HashSet<&[u8]> = accepted.iter().map(|k| k.as_slice()).collect();
		let mut evictions: Vec<MultiEviction> = Vec::new();
		let mut persists: Vec<MultiPersist> = Vec::new();

		let mut dropped = 0usize;
		for (kind, persistent_object, (to_persist, to_drop)) in plan {
			self.refresh_read_tier(persistent_object, &to_persist, &to_drop, &accepted);
			if persistent_object {
				for (key, _, value) in &to_persist {
					if accepted_keys.contains(key.as_slice()) {
						persists.push(MultiPersist {
							key: key.clone(),
							value_bytes: ByteSize::from_bytes(
								value.as_ref().map(|v| v.len() as u64).unwrap_or(0),
							),
						});
					}
				}
			}
			for evicted in &to_drop {
				evictions.push(MultiEviction {
					key: evicted.key.clone(),
					value_bytes: evicted.value_bytes,
					current: evicted.current,
				});
			}
			if let Some(count) = self.drop_from_commit(kind, to_drop) {
				dropped += count;
			}
		}

		if !evictions.is_empty() || !persists.is_empty() {
			self.event_bus.emit(MultiSweptEvent::new(evictions, persists, cutoff));
		}

		if persisted > 0 || dropped > 0 {
			debug!(cutoff = cutoff.0, persisted, dropped, more, "flush sweep slice completed");
		}

		let progress = if more {
			Progress::Yielded
		} else {
			Progress::Exhausted
		};
		(progress, dropped as u64)
	}

	#[inline]
	fn list_evictable_kinds(&self) -> Option<Vec<EntryKind>> {
		match self.commit.list_entry_kinds_by_oldest_pending() {
			Ok(v) => Some(v),
			Err(e) => {
				warn!(error = %e, "flush sweep: list_entry_kinds_by_oldest_pending failed");
				None
			}
		}
	}

	#[inline]
	fn collect_evictable(
		&self,
		kind: EntryKind,
		cutoff: CommitVersion,
		budget: usize,
	) -> (EvictablePersist, EvictableDrop, bool) {
		match &self.commit {
			MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff, budget),
		}
	}

	#[inline]
	fn refresh_read_tier(
		&self,
		persistent_object: bool,
		to_persist: &[(EncodedKey, CommitVersion, Option<CowVec<u8>>)],
		to_drop: &[EvictedVersion],
		accepted: &[EncodedKey],
	) {
		let Some(read) = &self.read else {
			return;
		};
		if persistent_object {
			let accepted: HashSet<&[u8]> = accepted.iter().map(|k| k.as_slice()).collect();
			for (key, version, value) in to_persist {
				if accepted.contains(key.as_slice()) {
					read.insert(key.clone(), *version, value.clone());
				} else {
					read.invalidate(key);
				}
			}
		} else {
			for evicted in to_drop {
				read.invalidate(&evicted.key);
			}
		}
	}

	#[inline]
	fn drop_from_commit(&self, kind: EntryKind, to_drop: EvictableDrop) -> Option<usize> {
		let drop_count = to_drop.len();
		reifydb_assertions! {
			assert!(
				drop_count > 0,
				"sweep must only reach drop_from_commit with a non-empty drop set; an empty drop \
				 issues a no-op commit-buffer drop and lets the dropped counter run for zero work \
				 (kind={kind:?})"
			);
		}
		let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		batches.insert(kind, to_drop.into_iter().map(|e| (e.key, e.version)).collect());
		if let Err(e) = self.commit.compact(batches) {
			warn!(?kind, error = %e, "flush sweep: commit buffer drop failed");
			return None;
		}
		Some(drop_count)
	}
}

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod tests {
	use reifydb_core::{
		event::EventListener,
		interface::catalog::{id::TableId, storage::StorageId},
	};
	use reifydb_runtime::{actor::system::ActorSystem, shutdown::Shutdown};
	use reifydb_sqlite::SqliteTempPathGuard;
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::tier::{VersionedGetResult, read::ReadBufferConfig};

	fn ek(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn val(s: &str) -> CowVec<u8> {
		CowVec::new(s.as_bytes().to_vec())
	}

	fn write(buffer: &MultiCommitBufferTier, kind: EntryKind, key: &EncodedKey, version: u64, value: &str) {
		buffer.set(CommitVersion(version), HashMap::from([(kind, vec![(key.clone(), Some(val(value)))])]))
			.unwrap();
	}

	struct StaticWatermark(CommitVersion);

	impl EvictionWatermark for StaticWatermark {
		fn watermark(&self) -> CommitVersion {
			self.0
		}
	}

	struct AllPersistent;

	impl ObjectPersistence for AllPersistent {
		fn is_persistent(&self, _storage: StorageId) -> bool {
			true
		}
	}

	struct NonePersistent;

	impl ObjectPersistence for NonePersistent {
		fn is_persistent(&self, _storage: StorageId) -> bool {
			false
		}
	}

	fn build_engine(
		persistence: Arc<dyn ObjectPersistence>,
		watermark: Option<CommitVersion>,
	) -> (FlushEngine, SqliteTempPathGuard) {
		let buffer = MultiCommitBufferTier::memory();
		let (persistent, guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(persistence);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		if let Some(w) = watermark {
			*watermark_lock.write() = Some(Arc::new(StaticWatermark(w)));
		}
		(
			FlushEngine::new(
				buffer,
				persistent,
				persistence_lock,
				watermark_lock,
				None,
				Clock::Real,
				testing_event_bus(),
			),
			guard,
		)
	}

	fn testing_event_bus() -> EventBus {
		EventBus::new(&ActorSystem::testing(Clock::testing()).spawner())
	}

	#[derive(Clone, Default)]
	struct SweepCollector {
		events: Arc<Mutex<Vec<MultiSweptEvent>>>,
	}

	impl EventListener<MultiSweptEvent> for SweepCollector {
		fn on(&self, event: &MultiSweptEvent) {
			self.events.lock().push(event.clone());
		}
	}

	fn build_engine_watching_sweeps(
		persistence: Arc<dyn ObjectPersistence>,
		watermark: CommitVersion,
	) -> (FlushEngine, SqliteTempPathGuard, SweepCollector) {
		let buffer = MultiCommitBufferTier::memory();
		let (persistent, guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(persistence);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		*watermark_lock.write() = Some(Arc::new(StaticWatermark(watermark)));

		let event_bus = testing_event_bus();
		let collector = SweepCollector::default();
		event_bus.register::<MultiSweptEvent, _>(collector.clone());

		(
			FlushEngine::new(
				buffer,
				persistent,
				persistence_lock,
				watermark_lock,
				None,
				Clock::Real,
				event_bus,
			),
			guard,
			collector,
		)
	}

	#[test]
	fn a_sweep_reports_every_version_it_evicted_from_the_commit_buffer() {
		let (engine, _guard, collector) =
			build_engine_watching_sweeps(Arc::new(AllPersistent), CommitVersion(2));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		let key = ek("k");

		write(&engine.commit, kind, &key, 1, "v1");
		write(&engine.commit, kind, &key, 2, "v2");

		engine.sweep(CommitVersion(2));
		engine.event_bus.wait_for_completion();

		let events = collector.events.lock().clone();
		assert_eq!(events.len(), 1, "one sweep slice must report exactly once");

		let evictions = events[0].evictions();
		assert_eq!(evictions.len(), 2, "both versions left the buffer and both must be accounted");

		let current: Vec<&MultiEviction> = evictions.iter().filter(|e| e.current).collect();
		assert_eq!(current.len(), 1, "exactly one of the two was the live version");
		assert_eq!(
			current[0].value_bytes,
			ByteSize::from_bytes(2),
			"the evicted bytes must be the value's own, not a placeholder"
		);

		let superseded: Vec<&MultiEviction> = evictions.iter().filter(|e| !e.current).collect();
		assert_eq!(superseded.len(), 1, "v1 was superseded by v2 and is discarded, not persisted");

		let persists = events[0].persists();
		assert_eq!(persists.len(), 1, "only the latest version below the cutoff reaches the persistent tier");
		assert_eq!(persists[0].value_bytes, ByteSize::from_bytes(2));
	}

	#[test]
	fn a_sweep_that_persists_nothing_still_reports_what_it_discarded() {
		let (engine, _guard, collector) =
			build_engine_watching_sweeps(Arc::new(NonePersistent), CommitVersion(2));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		let key = ek("k");

		write(&engine.commit, kind, &key, 1, "v1");
		write(&engine.commit, kind, &key, 2, "v2");

		engine.sweep(CommitVersion(2));
		engine.event_bus.wait_for_completion();

		let events = collector.events.lock().clone();
		assert_eq!(events.len(), 1);
		assert_eq!(events[0].evictions().len(), 2, "the discarded versions are still reported");
		assert!(events[0].persists().is_empty(), "a non-persistent object persists nothing");
	}

	fn build_engine_with_read(
		persistence: Arc<dyn ObjectPersistence>,
		watermark: CommitVersion,
		read: MultiReadBufferTier,
	) -> (FlushEngine, SqliteTempPathGuard) {
		let buffer = MultiCommitBufferTier::memory();
		let (persistent, guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(persistence);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		*watermark_lock.write() = Some(Arc::new(StaticWatermark(watermark)));
		(
			FlushEngine::new(
				buffer,
				persistent,
				persistence_lock,
				watermark_lock,
				Some(read),
				Clock::Real,
				testing_event_bus(),
			),
			guard,
		)
	}

	#[test]
	fn eviction_cutoff_is_none_without_watermark() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), None);
		assert!(actor.eviction_cutoff().is_none(), "no watermark set => no eviction");
	}

	#[test]
	fn eviction_cutoff_is_none_at_zero() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(0)));
		assert!(actor.eviction_cutoff().is_none());
	}

	#[test]
	fn a_pinned_cutoff_reports_the_entries_it_could_not_release() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(StorageId::Table(TableId(1)));
		for i in 0..8u64 {
			write(&actor.commit, kind, &ek(&format!("k{i}")), 10 + i, "v");
		}

		let outcome = actor.sweep_slice(FLUSH_KEY_BUDGET);

		assert_eq!(outcome.reclaimed, 0, "nothing is below the pinned cutoff, so nothing can be reclaimed");
		assert_eq!(
			outcome.backlog, 8,
			"the entries the cutoff could not release must still be reported, or a pinned floor \
			 looks exactly like an idle one"
		);
		assert!(
			outcome.progress.is_exhausted(),
			"budget exhaustion must not be the backlog signal: a pinned cutoff collects nothing and \
			 therefore never reports more work to do"
		);
	}

	#[test]
	fn a_cutoff_that_can_release_reports_what_it_reclaimed() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(20)));
		let kind = EntryKind::Source(StorageId::Table(TableId(1)));
		for i in 0..8u64 {
			write(&actor.commit, kind, &ek(&format!("k{i}")), 10 + i, "v");
		}

		let outcome = actor.sweep_slice(FLUSH_KEY_BUDGET);

		assert!(outcome.reclaimed > 0, "entries below the cutoff must count as work done");
		assert_eq!(outcome.backlog, 0, "a drained buffer reports no backlog");
	}

	#[test]
	fn sweep_persists_then_evicts_persistent_object_below_watermark() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(StorageId::Table(TableId(1)));
		let key = ek("k");
		write(&actor.commit, kind, &key, 1, "v1");
		write(&actor.commit, kind, &key, 2, "v2");
		write(&actor.commit, kind, &key, 3, "v3");

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(
				actor.commit.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"v2 must be gone from the buffer after eviction"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::Value { .. }
			),
			"v2 must survive in the persistent tier"
		);

		assert_eq!(
			actor.commit.get(kind, key.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice()),
			"v3 (> cutoff) must stay in the buffer"
		);
	}

	#[test]
	fn sweep_evicts_non_persistent_object_without_persisting() {
		let (actor, _guard) = build_engine(Arc::new(NonePersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(StorageId::Table(TableId(7)));
		let key = ek("ephemeral");
		write(&actor.commit, kind, &key, 1, "v1");
		write(&actor.commit, kind, &key, 2, "v2");
		write(&actor.commit, kind, &key, 3, "v3");

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(
				actor.commit.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"non-persistent object must still be evicted below the watermark"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"non-persistent object must NOT be written to the persistent tier"
		);
		assert_eq!(
			actor.commit.get(kind, key.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice()),
			"v3 (> cutoff) must stay resident even for a non-persistent object"
		);
	}

	#[test]
	fn sweep_keeps_everything_when_all_above_watermark() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(StorageId::Table(TableId(3)));
		let key = ek("k");
		write(&actor.commit, kind, &key, 5, "v5");

		actor.sweep(CommitVersion(1));

		assert_eq!(
			actor.commit.get(kind, key.as_ref(), CommitVersion(5)).unwrap().value().as_deref(),
			Some(b"v5".as_slice()),
			"a version above the watermark must never be evicted"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(5)).unwrap(),
				VersionedGetResult::NotFound
			),
			"nothing below the watermark => nothing persisted"
		);
	}

	#[test]
	fn sweep_seeds_evicted_keys_into_the_read_tier() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		})
		.unwrap();
		let (actor, _guard) = build_engine_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(11)));
		let key = ek("k");
		write(&actor.commit, kind, &key, 1, "v1");
		write(&actor.commit, kind, &key, 2, "v2");

		read.insert(key.clone(), CommitVersion(2), Some(val("stale")));

		actor.sweep(CommitVersion(2));

		match read.get(&key, CommitVersion(2)) {
			VersionedGetResult::Value {
				value,
				..
			} => assert_eq!(
				value.as_ref(),
				val("v2").as_ref(),
				"the read tier must hold the persisted value, not the stale one"
			),
			other => panic!("the sweep must seed the evicted key into the read tier, got {other:?}"),
		}
	}

	#[test]
	fn sweep_seeds_tombstone_into_read_tier() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		})
		.unwrap();
		let (actor, _guard) = build_engine_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(21)));
		let key = ek("k");
		write(&actor.commit, kind, &key, 1, "v1");
		actor.commit.set(CommitVersion(2), HashMap::from([(kind, vec![(key.clone(), None)])])).unwrap();

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(read.get(&key, CommitVersion(2)), VersionedGetResult::Tombstone),
			"an evicted tombstone must be seeded into the read tier as a definitive miss, not left absent \
			 (which would fall through and risk resurrecting an older value)"
		);
	}

	#[test]
	fn sweep_invalidates_rejected_key_but_seeds_accepted() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		})
		.unwrap();
		let (actor, _guard) = build_engine_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(22)));
		let rejected = ek("rejected");
		let accepted = ek("accepted");

		actor.persistent
			.set(CommitVersion(3), HashMap::from([(kind, vec![(rejected.clone(), Some(val("high")))])]))
			.unwrap();

		read.insert(rejected.clone(), CommitVersion(2), Some(val("stale")));

		write(&actor.commit, kind, &rejected, 2, "low");
		write(&actor.commit, kind, &accepted, 2, "b");

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(read.get(&rejected, CommitVersion(2)), VersionedGetResult::NotFound),
			"a guard-rejected key must be invalidated in the read tier so reads fall through to the newer \
			 persisted value, never serving the stale entry"
		);
		match read.get(&accepted, CommitVersion(2)) {
			VersionedGetResult::Value {
				value,
				..
			} => assert_eq!(value.as_ref(), val("b").as_ref(), "the accepted key must be seeded"),
			other => panic!("the accepted key must be seeded into the read tier, got {other:?}"),
		}
	}

	#[test]
	fn sweep_seed_respects_read_tier_downgrade_guard() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		})
		.unwrap();
		let (actor, _guard) = build_engine_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(23)));
		let key = ek("k");

		read.insert(key.clone(), CommitVersion(5), Some(val("newer")));

		write(&actor.commit, kind, &key, 2, "older");
		actor.sweep(CommitVersion(2));

		match read.get(&key, CommitVersion(5)) {
			VersionedGetResult::Value {
				value,
				..
			} => assert_eq!(
				value.as_ref(),
				val("newer").as_ref(),
				"the older seeded value must not overwrite a newer resident read-tier entry"
			),
			other => panic!("the newer read-tier entry must survive the sweep's seed, got {other:?}"),
		}
	}

	#[test]
	fn sweep_invalidates_ephemeral_object_in_read_tier() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		})
		.unwrap();
		let (actor, _guard) = build_engine_with_read(Arc::new(NonePersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(24)));
		let key = ek("k");

		read.insert(key.clone(), CommitVersion(2), Some(val("stale")));
		write(&actor.commit, kind, &key, 2, "v2");

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(read.get(&key, CommitVersion(2)), VersionedGetResult::NotFound),
			"an ephemeral (persistent:false) object must be invalidated in the read tier, never seeded"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"an ephemeral object must not be persisted"
		);
	}

	#[test]
	fn sweep_seeds_accepted_keys_across_version_buckets() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		})
		.unwrap();
		let (actor, _guard) = build_engine_with_read(Arc::new(AllPersistent), CommitVersion(4), read.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(25)));
		let a = ek("a");
		let b = ek("b");
		write(&actor.commit, kind, &a, 1, "a1");
		write(&actor.commit, kind, &a, 2, "a2");
		write(&actor.commit, kind, &b, 3, "b3");
		write(&actor.commit, kind, &b, 4, "b4");

		actor.sweep(CommitVersion(4));

		match read.get(&a, CommitVersion(4)) {
			VersionedGetResult::Value {
				value,
				..
			} => assert_eq!(value.as_ref(), val("a2").as_ref(), "a's latest-<=W (v2) must be seeded"),
			other => panic!("key a must be seeded across version buckets, got {other:?}"),
		}
		match read.get(&b, CommitVersion(4)) {
			VersionedGetResult::Value {
				value,
				..
			} => assert_eq!(value.as_ref(), val("b4").as_ref(), "b's latest-<=W (v4) must be seeded"),
			other => panic!("key b must be seeded across version buckets, got {other:?}"),
		}
	}

	#[test]
	fn sweep_persists_tombstone_so_deleted_keys_stay_deleted_after_eviction() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(StorageId::Table(TableId(12)));
		let key = ek("k");
		write(&actor.commit, kind, &key, 1, "v1");
		actor.commit.set(CommitVersion(2), HashMap::from([(kind, vec![(key.clone(), None)])])).unwrap();

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(
				actor.commit.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"both versions are gone from the buffer"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::Tombstone
			),
			"the persisted latest value must be the tombstone - the row must not resurrect"
		);
	}

	#[test]
	fn sweep_evicts_below_and_keeps_above_across_multiple_keys() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(StorageId::Table(TableId(13)));
		let cold = ek("cold");
		let hot = ek("hot");
		write(&actor.commit, kind, &cold, 1, "cold1");
		write(&actor.commit, kind, &hot, 4, "hot4");

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(
				actor.commit.get(kind, cold.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"cold (v1 <= cutoff) must be evicted from the buffer"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, cold.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::Value { .. }
			),
			"cold must survive in persistent"
		);
		assert_eq!(
			actor.commit.get(kind, hot.as_ref(), CommitVersion(4)).unwrap().value().as_deref(),
			Some(b"hot4".as_slice()),
			"hot (v4 > cutoff) must stay resident in the buffer"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, hot.as_ref(), CommitVersion(4)).unwrap(),
				VersionedGetResult::NotFound
			),
			"hot must not be persisted - it is above the watermark"
		);
	}

	#[test]
	fn flush_all_persists_every_key_regardless_of_watermark() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(StorageId::Table(TableId(101)));
		let cold = ek("cold");
		let hot = ek("hot");
		write(&actor.commit, kind, &cold, 2, "cold2");
		write(&actor.commit, kind, &hot, 50, "hot50");

		actor.sweep(CommitVersion(u64::MAX));

		assert_eq!(
			actor.persistent.get(kind, cold.as_ref(), CommitVersion(u64::MAX)).unwrap().value().as_deref(),
			Some(b"cold2".as_slice()),
			"a key committed above the watermark must be persisted by a full flush"
		);
		assert_eq!(
			actor.persistent.get(kind, hot.as_ref(), CommitVersion(u64::MAX)).unwrap().value().as_deref(),
			Some(b"hot50".as_slice()),
			"the latest committed value of every key must survive a full flush"
		);
		assert!(
			matches!(
				actor.commit.get(kind, hot.as_ref(), CommitVersion(u64::MAX)).unwrap(),
				VersionedGetResult::NotFound
			),
			"a full flush drains the buffer after persisting"
		);
	}

	#[test]
	fn sweep_aborts_and_keeps_buffer_when_persist_fails() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let row_kind = EntryKind::Source(StorageId::Table(TableId(31)));
		let dict_kind = EntryKind::Multi;
		let row_key = ek("row-referencing-id-7");
		let dict_key = ek("dictionary-entry-7");
		write(&actor.commit, row_kind, &row_key, 1, "id=7");
		write(&actor.commit, dict_kind, &dict_key, 1, "entry-7");

		actor.persistent.shutdown();
		actor.sweep(CommitVersion(2));

		assert_eq!(
			actor.commit.get(row_kind, row_key.as_ref(), CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"id=7".as_slice()),
			"a failed persist must leave the row write in the commit buffer, not drop the only copy"
		);
		assert_eq!(
			actor.commit.get(dict_kind, dict_key.as_ref(), CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"entry-7".as_slice()),
			"a failed persist must leave the dictionary write in the commit buffer, not drop the only copy"
		);
	}

	#[test]
	fn persist_sweep_errors_when_storage_is_shut_down() {
		let (persistent, _guard) = MultiPersistentTier::sqlite_in_memory();
		persistent.shutdown();

		let kind = EntryKind::Source(StorageId::Table(TableId(32)));
		let batches = vec![(CommitVersion(1), HashMap::from([(kind, vec![(ek("k"), Some(val("v")))])]))];
		assert!(
			persistent.persist_sweep(batches).is_err(),
			"a shut-down persistent tier must refuse the sweep loudly so the buffer is not dropped"
		);
	}

	#[test]
	fn sweep_persists_all_kinds_and_versions_together() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(3)));
		let row_kind = EntryKind::Source(StorageId::Table(TableId(33)));
		let dict_kind = EntryKind::Multi;
		let row_key = ek("row-referencing-id-9");
		let dict_key = ek("dictionary-entry-9");
		write(&actor.commit, row_kind, &row_key, 3, "id=9");
		write(&actor.commit, dict_kind, &dict_key, 2, "entry-9");

		actor.sweep(CommitVersion(3));

		assert_eq!(
			actor.persistent.get(row_kind, row_key.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"id=9".as_slice()),
			"the row write must be durable after the sweep"
		);
		assert_eq!(
			actor.persistent
				.get(dict_kind, dict_key.as_ref(), CommitVersion(3))
				.unwrap()
				.value()
				.as_deref(),
			Some(b"entry-9".as_slice()),
			"the dictionary write committed at an earlier version must be durable in the same sweep"
		);
		assert!(
			matches!(
				actor.commit.get(row_kind, row_key.as_ref(), CommitVersion(3)).unwrap(),
				VersionedGetResult::NotFound
			),
			"a persisted row write must be drained from the buffer"
		);
		assert!(
			matches!(
				actor.commit.get(dict_kind, dict_key.as_ref(), CommitVersion(3)).unwrap(),
				VersionedGetResult::NotFound
			),
			"a persisted dictionary write must be drained from the buffer"
		);
	}

	#[test]
	fn flush_all_persists_latest_tombstone_above_watermark() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(StorageId::Table(TableId(102)));
		let key = ek("k");
		write(&actor.commit, kind, &key, 5, "v5");
		actor.commit.set(CommitVersion(9), HashMap::from([(kind, vec![(key.clone(), None)])])).unwrap();

		actor.sweep(CommitVersion(u64::MAX));

		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(u64::MAX)).unwrap(),
				VersionedGetResult::Tombstone
			),
			"a delete committed above the watermark must persist as a tombstone, not resurrect"
		);
	}

	#[test]
	fn sweep_persists_multi_kind_entries() {
		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(10)));
		let kind = EntryKind::Multi;
		let key = ek("dictionary-entry");
		write(&actor.commit, kind, &key, 5, "mint-id-7");

		actor.sweep(CommitVersion(10));

		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(10)).unwrap(),
				VersionedGetResult::Value { .. }
			),
			"a Multi entry committed below the watermark must reach the persistent tier; \
			 dictionary entries and CDC checkpoints live in this keyspace and are lost on restart if it does not"
		);
	}

	#[test]
	fn a_kind_behind_the_budget_prefix_is_still_swept_under_sustained_writes() {
		const KINDS: u64 = 40;
		const KEYS_PER_ROUND: u64 = 20;
		const BUDGET: usize = 40;
		const ROUNDS: u64 = 60;
		const FIRST_VERSION: u64 = 1;

		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(1_000_000)));
		let kinds: Vec<EntryKind> =
			(0..KINDS).map(|i| EntryKind::Source(StorageId::Table(TableId(i + 1)))).collect();

		let round_writes = |version: u64| {
			for kind in &kinds {
				for key in 0..KEYS_PER_ROUND {
					write(&actor.commit, *kind, &ek(&format!("v{version}-k{key}")), version, "x");
				}
			}
		};

		round_writes(FIRST_VERSION);
		let mut exhausted = 0;
		for round in 0..ROUNDS {
			if actor.sweep_slice(BUDGET).progress.is_yielded() {
				exhausted += 1;
			}
			round_writes(FIRST_VERSION + 1 + round);
		}

		assert_eq!(
			exhausted, ROUNDS,
			"the budget must run out every slice for this to exercise starvation at all"
		);

		let oldest = actor.commit.oldest_pending_version().expect("writes are still pending");
		assert!(
			oldest.0 > FIRST_VERSION,
			"the oldest pending version is still {} after {ROUNDS} slices, so at least one of the \
			 {KINDS} kinds was never swept once; that kind pins the durable frontier at the first \
			 write, which clamps the tombstone reap cutoff to zero and leaves every tombstone in the \
			 persistent tier undeletable",
			oldest.0
		);
	}

	#[test]
	fn a_kind_that_sorts_behind_a_deeper_backlog_is_still_reached_by_the_sweep() {
		const HOT_KINDS: u64 = 3;
		const KEYS_PER_ROUND: u64 = 40;
		const BUDGET: usize = 30;
		const ROUNDS: u64 = 80;
		const COLD_FIRST_VERSION: u64 = 50;

		let (actor, _guard) = build_engine(Arc::new(AllPersistent), Some(CommitVersion(1_000_000)));
		let hot: Vec<EntryKind> =
			(0..HOT_KINDS).map(|i| EntryKind::Source(StorageId::Table(TableId(i + 1)))).collect();
		let cold = EntryKind::Source(StorageId::Table(TableId(HOT_KINDS + 1)));

		for round in 1..=ROUNDS {
			for kind in &hot {
				for key in 0..KEYS_PER_ROUND {
					write(&actor.commit, *kind, &ek(&format!("v{round}-k{key}")), round, "x");
				}
			}
			if round == COLD_FIRST_VERSION {
				write(&actor.commit, cold, &ek("cold-key"), round, "x");
			}
			actor.sweep_slice(BUDGET);
		}

		assert!(
			actor.commit.oldest_pending_for(hot[0]).is_some(),
			"the hot kinds must stay backlogged, otherwise the budget never ran out and this exercises nothing"
		);
		assert_eq!(
			actor.commit.oldest_pending_for(cold),
			None,
			"the single write to the cold kind is still pending after {} slices, so the sweep never reached past the hot kinds sorted ahead of it",
			ROUNDS - COLD_FIRST_VERSION
		);
	}
}
