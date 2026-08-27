// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Commit tier of the multi-version store expressed over the shared [`CommitDomain`] driver. A slice is one
//! [`EntryKind`]'s latest-per-key values at or below the eviction watermark, plus every version of those keys
//! the watermark has aged out; the kinds are visited oldest-pending first so the durable frontier, which is the
//! minimum over kinds, can advance.
//!
//! The rows never leave the resident set at selection. Multi's commit buffer is a read path, not only a write
//! buffer, so a reader between select and persist must still resolve the swept versions from RAM; the buffer is
//! read under shared locks in [`CommitDomain::select`] and only compacted, by key and version, in
//! [`CommitDomain::settle`].

use std::{
	borrow::Cow,
	collections::{HashMap, HashSet},
	sync::{Arc, OnceLock},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	event::{
		EventBus,
		metric::{MultiEviction, MultiPersist, MultiSweptEvent},
	},
	interface::store::EntryKind,
	lifecycle::watermark::EvictionWatermark,
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_store::tier::commit::{CommitCensus, CommitDomain, CommitTier, Settlement, Slice};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, util::cowvec::CowVec};

use crate::{
	flush::ObjectPersistence,
	tier::{
		TierBatch,
		commit::{buffer::MultiCommitBufferTier, memory::storage::EvictedVersion},
		persistent::MultiPersistentTier,
		point::MultiPointTier,
		range::MultiRangeTier,
	},
};

pub type MultiCommitTier = CommitTier<MultiDomain>;

type PersistPlan = Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>;

pub struct MultiState {
	commit: MultiCommitBufferTier,
	persistent: MultiPersistentTier,
	persistence: Arc<OnceLock<Arc<dyn ObjectPersistence>>>,
	eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,
	point: Option<MultiPointTier>,
	range: Option<MultiRangeTier>,
	event_bus: EventBus,
}

impl MultiState {
	pub fn new(
		commit: MultiCommitBufferTier,
		persistent: MultiPersistentTier,
		persistence: Arc<OnceLock<Arc<dyn ObjectPersistence>>>,
		eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,
		event_bus: EventBus,
	) -> Self {
		Self {
			commit,
			persistent,
			persistence,
			eviction_watermark,
			point: None,
			range: None,
			event_bus,
		}
	}

	pub fn with_point(mut self, point: Option<MultiPointTier>) -> Self {
		self.point = point;
		self
	}

	pub fn with_range(mut self, range: Option<MultiRangeTier>) -> Self {
		self.range = range;
		self
	}

	pub fn commit(&self) -> &MultiCommitBufferTier {
		&self.commit
	}

	pub fn persistent(&self) -> &MultiPersistentTier {
		&self.persistent
	}

	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}

	fn is_persistent_object(&self, kind: EntryKind) -> bool {
		match kind {
			EntryKind::Source(storage) | EntryKind::PartitionedSource(storage) => {
				self.persistence.get().map(|provider| provider.is_persistent(storage)).unwrap_or(true)
			}
			EntryKind::Multi => true,
		}
	}

	fn refresh_read_tier(&self, batch: &MultiBatch, accepted: &[EncodedKey]) {
		if self.point.is_none() && self.range.is_none() {
			return;
		}
		if batch.persistent_object {
			let accepted: HashSet<&[u8]> = accepted.iter().map(|key| key.as_slice()).collect();
			for (key, version, value) in &batch.to_persist {
				if accepted.contains(key.as_slice()) {
					if let Some(range) = &self.range {
						range.insert(key.clone(), *version, value.clone());
					}
					if let Some(point) = &self.point {
						point.insert(key.clone(), *version, value.clone());
					}
				} else {
					if let Some(range) = &self.range {
						range.invalidate(key);
					}
					if let Some(point) = &self.point {
						point.invalidate(key);
					}
				}
			}
		} else {
			for evicted in &batch.to_drop {
				if let Some(range) = &self.range {
					range.invalidate(&evicted.key);
				}
				if let Some(point) = &self.point {
					point.invalidate(&evicted.key);
				}
			}
		}
	}

	fn emit_swept(&self, batch: &MultiBatch, accepted: &[EncodedKey]) {
		let accepted: HashSet<&[u8]> = accepted.iter().map(|key| key.as_slice()).collect();
		let mut persists: Vec<MultiPersist> = Vec::new();
		if batch.persistent_object {
			for (key, _, value) in &batch.to_persist {
				if accepted.contains(key.as_slice()) {
					persists.push(MultiPersist {
						key: key.clone(),
						value_bytes: ByteSize::from_bytes(
							value.as_ref().map(|bytes| bytes.len() as u64).unwrap_or(0),
						),
					});
				}
			}
		}
		let evictions: Vec<MultiEviction> = batch
			.to_drop
			.iter()
			.map(|evicted| MultiEviction {
				key: evicted.key.clone(),
				value_bytes: evicted.value_bytes,
				current: evicted.current,
			})
			.collect();
		if evictions.is_empty() && persists.is_empty() {
			return;
		}
		self.event_bus.emit(MultiSweptEvent::new(evictions, persists, batch.cutoff));
	}

	fn drop_from_commit(&self, kind: EntryKind, to_drop: Vec<EvictedVersion>) -> u64 {
		let drop_count = to_drop.len() as u64;
		reifydb_assertions! {
			assert!(
				drop_count > 0,
				"a settled slice must carry at least one version to drop; an empty drop set issues a \
				 no-op commit-buffer compaction and releases bytes nothing gave back (kind={kind:?})"
			);
		}
		let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		batches.insert(kind, to_drop.into_iter().map(|evicted| (evicted.key, evicted.version)).collect());
		self.commit.compact(batches).expect("multi commit tier could not compact a settled slice");
		drop_count
	}
}

pub struct MultiBatch {
	kind: EntryKind,
	persistent_object: bool,
	to_persist: PersistPlan,
	to_drop: Vec<EvictedVersion>,
	cutoff: CommitVersion,
	bytes: ByteSize,
}

#[derive(Clone, Copy, Debug)]
pub struct MultiDomain;

impl CommitDomain for MultiDomain {
	type State = MultiState;
	type Batch = MultiBatch;
	type Ack = Vec<EncodedKey>;
	type Cutoff = CommitVersion;
	type Kind = EntryKind;

	const SCOPE: &'static str = "store::multi::commit";

	const MAX_SLICES_PER_TICK: usize = usize::MAX;

	fn cutoff(state: &Self::State) -> Option<Self::Cutoff> {
		let cutoff = state.eviction_watermark.read().as_ref()?.watermark();
		if cutoff.0 == 0 {
			return None;
		}
		Some(cutoff)
	}

	fn cutoff_all() -> Self::Cutoff {
		CommitVersion(u64::MAX)
	}

	fn kinds(state: &Self::State) -> Vec<Self::Kind> {
		state.commit
			.list_entry_kinds_by_oldest_pending()
			.expect("multi commit tier could not rank its kinds by oldest pending version")
	}

	fn select(
		state: &Self::State,
		kind: Self::Kind,
		cutoff: Self::Cutoff,
		budget: ByteSize,
	) -> Option<Slice<Self>> {
		let MultiCommitBufferTier::Memory(storage) = &state.commit;
		let (to_persist, to_drop, consumed, more) = storage.collect_evictable_below(kind, cutoff, budget);
		if to_persist.is_empty() && to_drop.is_empty() {
			return None;
		}
		Some(Slice {
			batch: MultiBatch {
				kind,
				persistent_object: state.is_persistent_object(kind),
				to_persist,
				to_drop,
				cutoff,
				bytes: consumed,
			},
			bytes: consumed,
			more,
		})
	}

	fn persist(state: &Self::State, batch: &Self::Batch) -> reifydb_value::Result<Self::Ack> {
		let accepted = if batch.persistent_object && !batch.to_persist.is_empty() {
			let mut versioned: HashMap<CommitVersion, TierBatch> = HashMap::new();
			for (key, version, value) in &batch.to_persist {
				versioned.entry(*version)
					.or_default()
					.entry(batch.kind)
					.or_default()
					.push((key.clone(), value.clone()));
			}
			state.persistent.persist_sweep(versioned.into_iter().collect())?
		} else {
			Vec::new()
		};
		state.refresh_read_tier(batch, &accepted);
		Ok(accepted)
	}

	fn settle(state: &Self::State, batch: Self::Batch, ack: Self::Ack) -> Settlement {
		state.emit_swept(&batch, &ack);
		state.drop_from_commit(batch.kind, batch.to_drop);
		Settlement {
			released: batch.bytes,
			entries: ack.len() as u64,
		}
	}

	fn resident_bytes(state: &Self::State) -> ByteSize {
		state.commit.current_resident_bytes().saturating_add(state.commit.historical_resident_bytes())
	}

	fn kind_name(kind: Self::Kind) -> Cow<'static, str> {
		match kind {
			EntryKind::Multi => Cow::Borrowed("multi"),
			EntryKind::Source(storage) => Cow::Owned(format!("source::{storage}")),
			EntryKind::PartitionedSource(storage) => Cow::Owned(format!("partitioned::{storage}")),
		}
	}

	fn census(state: &Self::State) -> CommitCensus {
		state.commit.census()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		event::EventListener,
		interface::catalog::{id::TableId, storage::StorageId},
	};
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, sync::mutex::Mutex};
	use reifydb_sqlite::SqliteTempPathGuard;
	use reifydb_store::tier::commit::CommitConfig;

	use super::*;
	use crate::tier::{
		TierStorage, VersionedGetResult, commit::memory::storage::MemoryRowStorage, point::MultiPointConfig,
	};

	fn ek(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn val(s: &str) -> CowVec<u8> {
		CowVec::new(s.as_bytes().to_vec())
	}

	fn write(tier: &MultiCommitTier, kind: EntryKind, key: &EncodedKey, version: u64, value: &str) {
		tier.state()
			.commit()
			.set(CommitVersion(version), HashMap::from([(kind, vec![(key.clone(), Some(val(value)))])]))
			.unwrap();
	}

	fn budget_for(keys: &[String], value: &str) -> ByteSize {
		// A starvation scenario is defined by how many keys one slice may buy, so the budget is measured
		// from the real cost of those keys rather than guessed; a guess drifts the moment the charge changes.
		let storage = MemoryRowStorage::new();
		for key in keys {
			storage.set(
				CommitVersion(1),
				HashMap::from([(EntryKind::Multi, vec![(ek(key), Some(val(value)))])]),
			)
			.unwrap();
		}
		let (_, _, consumed, _) = storage.collect_evictable_below(
			EntryKind::Multi,
			CommitVersion(1),
			ByteSize::from_bytes(u64::MAX),
		);
		consumed
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

	fn testing_event_bus() -> EventBus {
		EventBus::new(&ActorSystem::testing(Clock::testing()).spawner())
	}

	fn build_tier(
		persistence: Arc<dyn ObjectPersistence>,
		watermark: Option<CommitVersion>,
		event_bus: EventBus,
	) -> (MultiCommitTier, SqliteTempPathGuard) {
		let commit = MultiCommitBufferTier::memory();
		let (persistent, guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(persistence);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		if let Some(version) = watermark {
			*watermark_lock.write() = Some(Arc::new(StaticWatermark(version)));
		}
		let tier = CommitTier::new(CommitConfig::testing(), |_budget| {
			MultiState::new(commit, persistent, persistence_lock, watermark_lock, event_bus)
		})
		.expect("the testing config carries a budget");
		(tier, guard)
	}

	fn tier(persistence: Arc<dyn ObjectPersistence>, watermark: Option<CommitVersion>) -> (MultiCommitTier, SqliteTempPathGuard) {
		build_tier(persistence, watermark, testing_event_bus())
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

	fn tier_with_point(
		persistence: Arc<dyn ObjectPersistence>,
		watermark: CommitVersion,
		point: MultiPointTier,
	) -> (MultiCommitTier, SqliteTempPathGuard) {
		let commit = MultiCommitBufferTier::memory();
		let (persistent, guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(persistence);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		*watermark_lock.write() = Some(Arc::new(StaticWatermark(watermark)));
		let tier = CommitTier::new(CommitConfig::testing(), |_budget| {
			MultiState::new(commit, persistent, persistence_lock, watermark_lock, testing_event_bus())
				.with_point(Some(point))
		})
		.expect("the testing config carries a budget");
		(tier, guard)
	}

	#[test]
	fn a_slice_with_no_resume_point_serves_the_kind_holding_the_oldest_pending_write() {
		// Isolates the oldest-pending ranking from the resume cursor, which otherwise covers for it. Each
		// tier is sliced exactly once, so the cursor is provably unset and cannot influence the order; the
		// ranking alone decides which kind a one-entry budget buys. Rotating which kind holds the old write
		// is what makes an unranked traversal fail: a fixed order serves the same kind every arrangement and
		// so can match the designated one at most once.
		const KINDS: u64 = 6;
		const OLD_VERSION: u64 = 1;
		const YOUNG_VERSION: u64 = 100;

		let budget = budget_for(&["k".to_string()], "v");

		for oldest in 0..KINDS {
			let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(1_000_000)));
			let kinds: Vec<EntryKind> =
				(0..KINDS).map(|i| EntryKind::Source(StorageId::Table(TableId(i + 1)))).collect();
			for (index, kind) in kinds.iter().enumerate() {
				let version = if index as u64 == oldest {
					OLD_VERSION
				} else {
					YOUNG_VERSION
				};
				write(&tier, *kind, &ek("k"), version, "v");
			}

			tier.flush_slice(budget);

			assert_eq!(
				tier.state().commit().oldest_pending_for(kinds[oldest as usize]),
				None,
				"a budget covering exactly one entry must buy the kind holding the oldest pending \
				 write, but the kind at version {OLD_VERSION} was left pending while a younger kind \
				 took the slice; the durable frontier is the minimum over kinds, so serving anything \
				 but the oldest cannot advance it"
			);
			for (index, kind) in kinds.iter().enumerate() {
				if index as u64 == oldest {
					continue;
				}
				assert_eq!(
					tier.state().commit().oldest_pending_for(*kind),
					Some(CommitVersion(YOUNG_VERSION)),
					"the budget covered one entry, so no kind younger than the oldest may have \
					 been served in the same slice"
				);
			}
		}
	}

	#[test]
	fn a_slice_cut_off_mid_kind_resumes_past_it_instead_of_restarting_at_the_head() {
		// Isolates the resume cursor from the oldest-pending ranking, which otherwise covers for it. The
		// deep kind holds the oldest pending version for the whole run, so the ranking alone puts it at the
		// head of every slice and the budget dies inside it every time; nothing but a cursor that resumes
		// past the kind it was cut off in can reach the cold kind behind it. The ranking is left intact here,
		// so a cold write still pending at the end can only mean the cursor is gone.
		const DEEP_KEYS: u64 = 400;
		const KEYS_PER_SLICE: u64 = 10;
		const SLICES: u64 = 20;
		const COLD_VERSION: u64 = 50;

		let budget = budget_for(&(0..KEYS_PER_SLICE).map(|i| format!("deep-k{i}")).collect::<Vec<_>>(), "x");

		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(1_000_000)));
		let deep = EntryKind::Source(StorageId::Table(TableId(1)));
		let cold = EntryKind::Source(StorageId::Table(TableId(2)));

		for key in 0..DEEP_KEYS {
			write(&tier, deep, &ek(&format!("deep-k{key}")), 1, "x");
		}
		write(&tier, cold, &ek("cold-k"), COLD_VERSION, "x");

		for _ in 0..SLICES {
			tier.flush_slice(budget);
		}

		assert_eq!(
			tier.state().commit().oldest_pending_for(deep),
			Some(CommitVersion(1)),
			"the deep kind must still hold the oldest pending version after every slice, otherwise it \
			 stopped ranking ahead of the cold kind and this never exercised the cursor at all"
		);
		assert_eq!(
			tier.state().commit().oldest_pending_for(cold),
			None,
			"the cold write is still pending after {SLICES} slices, so every slice restarted at the deep \
			 kind the ranking puts first and never resumed past the kind it was cut off in"
		);
	}

	#[test]
	fn a_kind_behind_the_budget_prefix_is_still_swept_under_sustained_writes() {
		// The two mechanisms together, at the level the guarantee is actually stated: no kind may be starved
		// while writes keep arriving. A kind never reached pins the durable frontier at its first write,
		// which clamps the tombstone reap cutoff to zero.
		const KINDS: u64 = 40;
		const KEYS_PER_ROUND: u64 = 20;
		const KEYS_PER_SLICE: u64 = 40;
		const ROUNDS: u64 = 60;
		const FIRST_VERSION: u64 = 1;

		let budget = budget_for(&(0..KEYS_PER_SLICE).map(|i| format!("v1-k{i}")).collect::<Vec<_>>(), "x");

		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(1_000_000)));
		let kinds: Vec<EntryKind> =
			(0..KINDS).map(|i| EntryKind::Source(StorageId::Table(TableId(i + 1)))).collect();

		let round_writes = |version: u64| {
			for kind in &kinds {
				for key in 0..KEYS_PER_ROUND {
					write(&tier, *kind, &ek(&format!("v{version}-k{key}")), version, "x");
				}
			}
		};

		round_writes(FIRST_VERSION);
		let mut yielded = 0;
		for round in 0..ROUNDS {
			if tier.flush_slice(budget).is_yielded() {
				yielded += 1;
			}
			round_writes(FIRST_VERSION + 1 + round);
		}

		assert_eq!(
			yielded, ROUNDS,
			"the budget must run out every slice for this to exercise starvation at all"
		);

		let oldest = tier.state().commit().oldest_pending_version().expect("writes are still pending");
		assert!(
			oldest.0 > FIRST_VERSION,
			"the oldest pending version is still {} after {ROUNDS} slices, so at least one of the \
			 {KINDS} kinds was never swept once",
			oldest.0
		);
	}

	#[test]
	fn a_version_above_the_watermark_stays_resident() {
		// The commit buffer is the only place a version above the watermark exists; sweeping one persists a
		// row readers at older snapshots must not see yet.
		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));

		write(&tier, kind, &ek("below"), 1, "a");
		write(&tier, kind, &ek("above"), 5, "b");

		tier.flush_pending();

		assert_eq!(
			tier.state().commit().oldest_pending_for(kind),
			Some(CommitVersion(5)),
			"the write above the watermark must still be pending, and the one below must be gone"
		);
		let idle = tier.flush_pending();
		assert_eq!(
			idle.slices, 0,
			"a kind whose whole backlog sits above the cutoff must yield no slice at all; settling an \
			 empty one releases bytes nothing gave back and runs a no-op compaction every tick"
		);
		assert!(
			matches!(
				tier.state().persistent().get(kind, b"above", CommitVersion(10)).unwrap(),
				VersionedGetResult::NotFound
			),
			"a version above the watermark must not reach the persistent tier"
		);
	}

	#[test]
	fn no_watermark_flushes_nothing() {
		// A store whose watermark has not been injected yet must not treat "no bound" as "no bound needed";
		// sweeping under an open cutoff there would persist versions still visible to live readers.
		let (tier, _guard) = tier(Arc::new(AllPersistent), None);
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		write(&tier, kind, &ek("k"), 1, "v");

		let outcome = tier.flush_pending();

		assert!(outcome.is_exhausted());
		assert_eq!(outcome.slices, 0);
		assert_eq!(
			tier.state().commit().oldest_pending_for(kind),
			Some(CommitVersion(1)),
			"nothing may be swept while no eviction watermark exists"
		);
	}

	#[test]
	fn a_drain_empties_the_resident_set_including_versions_above_the_watermark() {
		// Shutdown is the only caller that must leave nothing behind: whatever the drain holds back is lost,
		// because the commit buffer is RAM only.
		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		write(&tier, kind, &ek("a"), 1, "x");
		write(&tier, kind, &ek("b"), 9_000, "y");

		tier.flush_all();

		assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a drain must leave the resident set empty");
		assert!(
			matches!(
				tier.state().persistent().get(kind, b"b", CommitVersion(9_000)).unwrap(),
				VersionedGetResult::Value { .. }
			),
			"the drained version above the watermark must be durable, not discarded"
		);
	}

	#[test]
	fn only_the_latest_version_below_the_cutoff_is_persisted() {
		// Superseded versions are what the buffer collapses; persisting them would write rows no reader can
		// ever resolve to, and the whole point of the window is that they never reach the device.
		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(3)));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		let key = ek("k");
		for version in 1..=3u64 {
			write(&tier, kind, &key, version, &format!("v{version}"));
		}

		let outcome = tier.flush_pending();

		assert_eq!(outcome.persisted, 1, "three versions of one key must settle as one persisted row");
		assert_eq!(
			tier.state().persistent().get(kind, key.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);
		assert!(
			matches!(
				tier.state().persistent().get(kind, key.as_ref(), CommitVersion(1)).unwrap(),
				VersionedGetResult::NotFound
			),
			"the superseded versions must be discarded, not written"
		);
	}

	#[test]
	fn a_non_persistent_object_is_dropped_without_being_written() {
		// A transient object's rows exist only to be read back before eviction; writing them to the device is
		// exactly the amplification the persistence flag exists to avoid.
		let (tier, _guard) = tier(Arc::new(NonePersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		write(&tier, kind, &ek("k"), 1, "v");

		let outcome = tier.flush_pending();

		assert_eq!(outcome.persisted, 0, "nothing may be acknowledged for a non-persistent object");
		assert!(outcome.released.as_bytes() > 0, "its bytes must still be released");
		assert_eq!(tier.state().commit().oldest_pending_for(kind), None);
		assert!(matches!(
			tier.state().persistent().get(kind, b"k", CommitVersion(2)).unwrap(),
			VersionedGetResult::NotFound
		));
	}

	#[test]
	fn a_selected_row_stays_readable_until_it_settles() {
		// Multi's commit buffer is a read path, not only a write buffer. Between select and settle the
		// persistent tier does not hold the row yet, so a select that removed it would open a hole for every
		// reader for the length of the flush transaction.
		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		let key = ek("k");
		write(&tier, kind, &key, 1, "v");

		let slice = tier.take(kind, CommitVersion(1), ByteSize::from_mib(1)).expect("one key is evictable");
		assert_eq!(
			tier.state().commit().get(kind, key.as_ref(), CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"v".as_slice()),
			"the selected row must still resolve from the commit buffer while the batch is in flight"
		);

		let ack = MultiDomain::persist(tier.state(), &slice.batch).unwrap();
		tier.settle(slice.batch, ack);

		assert_eq!(
			tier.state().commit().oldest_pending_for(kind),
			None,
			"settling is what removes the row, and it must actually remove it"
		);
	}

	#[test]
	fn a_settle_releases_exactly_what_the_slice_charged() {
		// The budget is the only bound on the resident set, so a settle that releases more than the slice
		// charged re-arms the window early and one that releases less leaks the window shut.
		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(5)));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		for key in 0..8u64 {
			write(&tier, kind, &ek(&format!("k{key}")), 1, "value");
		}
		let before = tier.resident_bytes();

		let outcome = tier.flush_pending();

		assert_eq!(before, outcome.released, "the released bytes must equal what the resident set held");
		assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
	}

	#[test]
	fn the_byte_counter_never_drifts_from_a_walk_of_the_buffer() {
		// The counter is incremental and the walk is exhaustive; a drift above the walk hides bytes the
		// budget can never release, and one below lets the resident set grow past a window reporting itself
		// empty. Tombstones and superseded versions are included because each releases through a different path.
		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(4)));
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		let other = EntryKind::Source(StorageId::table(TableId(2)));

		for version in 1..=3u64 {
			write(&tier, kind, &ek("k"), version, &format!("value-{version}"));
		}
		write(&tier, other, &ek("j"), 2, "j");
		tier.state()
			.commit()
			.set(CommitVersion(4), HashMap::from([(kind, vec![(ek("k"), None)])]))
			.unwrap();
		write(&tier, kind, &ek("later"), 9, "still-resident");

		let mid = MultiDomain::census(tier.state());
		assert!(mid.walked.as_bytes() > 0, "precondition: the scenario must leave bytes resident");
		assert!(
			tier.state().commit().historical_resident_bytes().as_bytes() > 0,
			"precondition: superseded versions must be resident, or the historical charge goes unmeasured"
		);
		assert_eq!(mid.counted, mid.walked, "the counter must match a walk before any slice runs");
		assert_eq!(
			mid.counted,
			tier.resident_bytes(),
			"every resident layer must be charged, superseded versions included"
		);

		tier.flush_pending();

		let after = MultiDomain::census(tier.state());
		assert_eq!(after.counted, after.walked, "the counter must match a walk after the slice settles");
		assert_eq!(
			after.counted,
			tier.resident_bytes(),
			"the counter the budget reads must be the counter the census compares"
		);
		assert!(after.walked.as_bytes() > 0, "the write above the cutoff must still be charged");
	}

	#[test]
	fn a_settled_slice_reports_what_it_evicted_and_what_it_persisted() {
		// The swept event is how the retention plane learns the durable frontier moved; a slice that settles
		// silently leaves the reap cutoff pinned at whatever it was.
		let event_bus = testing_event_bus();
		let collector = SweepCollector::default();
		event_bus.register::<MultiSweptEvent, _>(collector.clone());
		let (tier, _guard) = build_tier(Arc::new(AllPersistent), Some(CommitVersion(2)), event_bus);
		let kind = EntryKind::Source(StorageId::table(TableId(1)));
		let key = ek("k");
		write(&tier, kind, &key, 1, "v1");
		write(&tier, kind, &key, 2, "v2");

		tier.flush_pending();
		tier.state().event_bus().wait_for_completion();

		let events = collector.events.lock().clone();
		assert_eq!(events.len(), 1, "one kind settled once, so exactly one event may be raised");
		assert_eq!(events[0].evictions().len(), 2, "both versions left the buffer and both must be accounted");
		assert_eq!(events[0].persists().len(), 1, "only the latest version below the cutoff was written");
		assert_eq!(*events[0].version(), CommitVersion(2), "the event must carry the cutoff it swept under");
	}

	#[test]
	fn each_kind_is_counted_under_its_own_metric_label() {
		// Per-kind counters are what made the starvation work legible; folding two kinds into one label hides
		// a kind that is never served.
		let (tier, _guard) = tier(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let first = EntryKind::Source(StorageId::table(TableId(1)));
		let second = EntryKind::Source(StorageId::table(TableId(2)));
		write(&tier, first, &ek("a"), 1, "x");
		write(&tier, second, &ek("b"), 1, "y");

		tier.flush_pending();

		let mut labels: Vec<String> =
			tier.kind_metrics().iter().map(|entry| MultiDomain::kind_name(entry.kind).into_owned()).collect();
		labels.sort();
		assert_eq!(labels, vec!["source::1".to_string(), "source::2".to_string()]);
		for entry in tier.kind_metrics() {
			assert_eq!(entry.counters.persisted, 1, "each kind persisted exactly its own row");
		}
	}

	#[test]
	fn a_persisted_value_is_seeded_into_the_read_tier() {
		// The row leaves RAM at settle, so a read tier still holding the pre-flush entry serves a value the
		// buffer no longer backs; seeding is what keeps the next read off the device.
		let point = MultiPointTier::new(MultiPointConfig::testing()).unwrap();
		let (tier, _guard) = tier_with_point(Arc::new(AllPersistent), CommitVersion(2), point.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(11)));
		let key = ek("k");
		write(&tier, kind, &key, 1, "v1");
		write(&tier, kind, &key, 2, "v2");
		point.insert(key.clone(), CommitVersion(2), Some(val("stale")));

		tier.flush_pending();

		match point.get(&key, CommitVersion(2)) {
			VersionedGetResult::Value {
				value,
				..
			} => assert_eq!(
				value.as_ref(),
				val("v2").as_ref(),
				"the point tier must hold the persisted value, not the entry it held before the flush"
			),
			other => panic!("the settled key must be seeded into the point tier, got {other:?}"),
		}
	}

	#[test]
	fn an_ephemeral_object_is_invalidated_in_the_read_tier_rather_than_seeded() {
		// Nothing was written, so a seeded entry would be the only copy of a row the store has agreed to
		// forget; the read tier must fall through instead.
		let point = MultiPointTier::new(MultiPointConfig::testing()).unwrap();
		let (tier, _guard) = tier_with_point(Arc::new(NonePersistent), CommitVersion(2), point.clone());
		let kind = EntryKind::Source(StorageId::Table(TableId(24)));
		let key = ek("k");
		point.insert(key.clone(), CommitVersion(2), Some(val("stale")));
		write(&tier, kind, &key, 2, "v2");

		tier.flush_pending();

		assert!(
			matches!(point.get(&key, CommitVersion(2)), VersionedGetResult::NotFound),
			"an object that persists nothing must be invalidated in the point tier, never seeded"
		);
	}

	#[test]
	fn one_flush_pending_drains_a_backlog_wider_than_a_single_slice() {
		// The lifecycle task calls this once per tick, so whatever one call leaves behind waits a whole tick;
		// a backlog arriving faster than one slice per tick then grows without bound.
		const KEYS: u64 = 12;

		let budget = budget_for(&["k0".to_string(), "k1".to_string()], "value");
		let commit = MultiCommitBufferTier::memory();
		let (persistent, _guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(Arc::new(AllPersistent) as Arc<dyn ObjectPersistence>);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		*watermark_lock.write() = Some(Arc::new(StaticWatermark(CommitVersion(1_000))));
		let tier = CommitTier::new(
			CommitConfig {
				budget: Some(budget),
				interval: CommitConfig::testing().interval,
			},
			|_budget| MultiState::new(commit, persistent, persistence_lock, watermark_lock, testing_event_bus()),
		)
		.expect("the config carries a budget");

		let kind = EntryKind::Source(StorageId::Table(TableId(1)));
		for key in 0..KEYS {
			write(&tier, kind, &ek(&format!("k{key}")), 1, "value");
		}

		let outcome = tier.flush_pending();

		assert!(outcome.slices > 1, "precondition: the backlog must not fit in one slice");
		assert_eq!(
			tier.resident_bytes(),
			ByteSize::ZERO,
			"one call must paginate until nothing below the cutoff is left"
		);
	}
}
