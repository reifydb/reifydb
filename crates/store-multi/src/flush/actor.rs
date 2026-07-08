// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_codec::key::encoded::EncodedKey;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::timers::TimerHandle;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	traits::{Actor, Directive},
};
use reifydb_runtime::sync::{rwlock::RwLock, waiter::WaiterHandle};
use reifydb_value::value::{datetime::DateTime, duration::Duration};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_value::{reifydb_assertions, util::cowvec::CowVec};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use tracing::{debug, error, warn};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::{TierBatch, TierStorage};
use crate::{
	flush::ShapePersistence,
	gc::EvictionWatermark,
	tier::{commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier, read::MultiReadBufferTier},
};

#[derive(Clone)]
pub enum FlushMessage {
	Tick(DateTime),
	Shutdown,

	SetInterval(Duration),

	FlushPending {
		waiter: Arc<WaiterHandle>,
	},

	FlushAll {
		waiter: Arc<WaiterHandle>,
	},
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub struct FlushActorState {
	timer_handle: Option<TimerHandle>,
}

#[allow(dead_code)]
pub struct FlushActor {
	commit: MultiCommitBufferTier,
	persistent: MultiPersistentTier,
	flush_interval: Duration,
	persistence: Arc<OnceLock<Arc<dyn ShapePersistence>>>,
	eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,
	read: Option<MultiReadBufferTier>,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
type EvictablePartition = (Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>, Vec<(EncodedKey, CommitVersion)>);

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl FlushActor {
	pub fn new(
		commit: MultiCommitBufferTier,
		persistent: MultiPersistentTier,
		flush_interval: Duration,
		persistence: Arc<OnceLock<Arc<dyn ShapePersistence>>>,
		eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,
		read: Option<MultiReadBufferTier>,
	) -> Self {
		Self {
			commit,
			persistent,
			flush_interval,
			persistence,
			eviction_watermark,
			read,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		commit: MultiCommitBufferTier,
		persistent: MultiPersistentTier,
		flush_interval: Duration,
		persistence: Arc<OnceLock<Arc<dyn ShapePersistence>>>,
		eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,
		read: Option<MultiReadBufferTier>,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(commit, persistent, flush_interval, persistence, eviction_watermark, read);
		spawner.spawn_coordination("persistent-flush", actor).actor_ref().clone()
	}

	fn eviction_cutoff(&self) -> Option<CommitVersion> {
		let cutoff = self.eviction_watermark.read().as_ref()?.watermark();
		if cutoff.0 == 0 {
			return None;
		}
		Some(cutoff)
	}

	fn is_persistent_shape(&self, kind: EntryKind) -> bool {
		match kind {
			EntryKind::Source(shape) | EntryKind::PartitionedSource(shape) => {
				self.persistence.get().map(|provider| provider.is_persistent(shape)).unwrap_or(true)
			}
			_ => true,
		}
	}

	fn sweep(&self, cutoff: CommitVersion) {
		let Some(entry_kinds) = self.list_evictable_kinds() else {
			return;
		};

		let mut plan: Vec<(EntryKind, bool, EvictablePartition)> = Vec::new();
		let mut batches: HashMap<CommitVersion, TierBatch> = HashMap::new();
		for kind in entry_kinds {
			let (to_persist, to_drop) = self.collect_evictable(kind, cutoff);
			if to_drop.is_empty() {
				continue;
			}
			let persistent_shape = self.is_persistent_shape(kind);
			if persistent_shape {
				for (key, version, value) in &to_persist {
					batches.entry(*version)
						.or_default()
						.entry(kind)
						.or_default()
						.push((key.clone(), value.clone()));
				}
			}
			plan.push((kind, persistent_shape, (to_persist, to_drop)));
		}
		if plan.is_empty() {
			return;
		}

		let accepted = if batches.values().any(|batch| !batch.is_empty()) {
			match self.persistent.persist_sweep(batches.into_iter().collect()) {
				Ok(accepted) => accepted,
				Err(e) => {
					error!(error = %e, "flush sweep: persist failed, aborting sweep");
					return;
				}
			}
		} else {
			Vec::new()
		};
		let persisted = accepted.len();

		let mut dropped = 0usize;
		for (kind, persistent_shape, (to_persist, to_drop)) in plan {
			self.refresh_read_tier(persistent_shape, &to_persist, &to_drop, &accepted);
			if let Some(count) = self.drop_from_commit(kind, to_drop) {
				dropped += count;
			}
		}

		self.checkpoint_and_maintain(cutoff, persisted, dropped);
	}

	#[inline]
	fn list_evictable_kinds(&self) -> Option<Vec<EntryKind>> {
		match self.commit.list_all_entry_kinds() {
			Ok(v) => Some(v),
			Err(e) => {
				warn!(error = %e, "flush sweep: list_all_entry_kinds failed");
				None
			}
		}
	}

	#[inline]
	fn collect_evictable(&self, kind: EntryKind, cutoff: CommitVersion) -> EvictablePartition {
		match &self.commit {
			MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff),
		}
	}

	#[inline]
	fn refresh_read_tier(
		&self,
		persistent_shape: bool,
		to_persist: &[(EncodedKey, CommitVersion, Option<CowVec<u8>>)],
		to_drop: &[(EncodedKey, CommitVersion)],
		accepted: &[EncodedKey],
	) {
		let Some(read) = &self.read else {
			return;
		};
		if persistent_shape {
			let accepted: HashSet<&[u8]> = accepted.iter().map(|k| k.as_slice()).collect();
			for (key, version, value) in to_persist {
				if accepted.contains(key.as_slice()) {
					read.insert(key.clone(), *version, value.clone());
				} else {
					read.invalidate(key);
				}
			}
		} else {
			for (key, _) in to_drop {
				read.invalidate(key);
			}
		}
	}

	#[inline]
	fn drop_from_commit(&self, kind: EntryKind, to_drop: Vec<(EncodedKey, CommitVersion)>) -> Option<usize> {
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
		batches.insert(kind, to_drop);
		if let Err(e) = self.commit.drop(batches) {
			warn!(?kind, error = %e, "flush sweep: commit buffer drop failed");
			return None;
		}
		Some(drop_count)
	}

	#[inline]
	fn checkpoint_and_maintain(&self, cutoff: CommitVersion, persisted: usize, dropped: usize) {
		if persisted > 0 || dropped > 0 {
			debug!(cutoff = cutoff.0, persisted, dropped, "flush sweep completed");
			if persisted > 0
				&& let Err(e) = self.persistent.maybe_checkpoint()
			{
				warn!(error = %e, "flush sweep: checkpoint failed");
			}
			self.commit.maintenance();
		}
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl Actor for FlushActor {
	type State = FlushActorState;
	type Message = FlushMessage;

	fn init(&self, ctx: &Context<FlushMessage>) -> FlushActorState {
		debug!("Persistent flush actor started");
		let timer_handle = ctx.schedule_tick(self.flush_interval.to_std(), |nanos| {
			FlushMessage::Tick(DateTime::from_nanos(nanos))
		});
		FlushActorState {
			timer_handle: Some(timer_handle),
		}
	}

	fn handle(&self, state: &mut FlushActorState, msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
		if ctx.is_cancelled() {
			if let Some(cutoff) = self.eviction_cutoff() {
				self.sweep(cutoff);
			}
			return Directive::Stop;
		}
		match msg {
			FlushMessage::Tick(_) => {
				if let Some(cutoff) = self.eviction_cutoff() {
					self.sweep(cutoff);
				}
			}
			FlushMessage::SetInterval(interval) => {
				if let Some(handle) = state.timer_handle.take() {
					handle.cancel();
				}
				state.timer_handle = Some(ctx.schedule_tick(interval.to_std(), |nanos| {
					FlushMessage::Tick(DateTime::from_nanos(nanos))
				}));
			}
			FlushMessage::Shutdown => {
				debug!("Persistent flush actor shutting down");
				if let Some(cutoff) = self.eviction_cutoff() {
					self.sweep(cutoff);
				}
				return Directive::Stop;
			}
			FlushMessage::FlushPending {
				waiter,
			} => {
				if let Some(cutoff) = self.eviction_cutoff() {
					self.sweep(cutoff);
				}
				waiter.notify();
			}
			FlushMessage::FlushAll {
				waiter,
			} => {
				self.sweep(CommitVersion(u64::MAX));
				waiter.notify();
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Persistent flush actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod tests {
	use reifydb_core::interface::catalog::{id::TableId, shape::ShapeId};
	use reifydb_runtime::shutdown::Shutdown;
	use reifydb_sqlite::SqliteTempPathGuard;
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::tier::{VersionedGetResult, read::ReadBufferConfig};

	fn ek(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
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

	impl ShapePersistence for AllPersistent {
		fn is_persistent(&self, _shape: ShapeId) -> bool {
			true
		}
	}

	struct NonePersistent;

	impl ShapePersistence for NonePersistent {
		fn is_persistent(&self, _shape: ShapeId) -> bool {
			false
		}
	}

	fn build_actor(
		persistence: Arc<dyn ShapePersistence>,
		watermark: Option<CommitVersion>,
	) -> (FlushActor, SqliteTempPathGuard) {
		let buffer = MultiCommitBufferTier::memory();
		let (persistent, guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ShapePersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(persistence);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		if let Some(w) = watermark {
			*watermark_lock.write() = Some(Arc::new(StaticWatermark(w)));
		}
		(
			FlushActor::new(
				buffer,
				persistent,
				Duration::from_seconds(5).unwrap(),
				persistence_lock,
				watermark_lock,
				None,
			),
			guard,
		)
	}

	fn build_actor_with_read(
		persistence: Arc<dyn ShapePersistence>,
		watermark: CommitVersion,
		read: MultiReadBufferTier,
	) -> (FlushActor, SqliteTempPathGuard) {
		let buffer = MultiCommitBufferTier::memory();
		let (persistent, guard) = MultiPersistentTier::sqlite_in_memory();
		let persistence_lock: Arc<OnceLock<Arc<dyn ShapePersistence>>> = Arc::new(OnceLock::new());
		let _ = persistence_lock.set(persistence);
		let watermark_lock: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));
		*watermark_lock.write() = Some(Arc::new(StaticWatermark(watermark)));
		(
			FlushActor::new(
				buffer,
				persistent,
				Duration::from_seconds(5).unwrap(),
				persistence_lock,
				watermark_lock,
				Some(read),
			),
			guard,
		)
	}

	#[test]
	fn eviction_cutoff_is_none_without_watermark() {
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), None);
		assert!(actor.eviction_cutoff().is_none(), "no watermark set => no eviction");
	}

	#[test]
	fn eviction_cutoff_is_none_at_zero() {
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(0)));
		assert!(actor.eviction_cutoff().is_none());
	}

	#[test]
	fn sweep_persists_then_evicts_persistent_shape_below_watermark() {
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(ShapeId::Table(TableId(1)));
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
	fn sweep_evicts_non_persistent_shape_without_persisting() {
		let (actor, _guard) = build_actor(Arc::new(NonePersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(ShapeId::Table(TableId(7)));
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
			"non-persistent shape must still be evicted below the watermark"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"non-persistent shape must NOT be written to the persistent tier"
		);
		assert_eq!(
			actor.commit.get(kind, key.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice()),
			"v3 (> cutoff) must stay resident even for a non-persistent shape"
		);
	}

	#[test]
	fn sweep_keeps_everything_when_all_above_watermark() {
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(ShapeId::Table(TableId(3)));
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
		});
		let (actor, _guard) = build_actor_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(ShapeId::Table(TableId(11)));
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
		});
		let (actor, _guard) = build_actor_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(ShapeId::Table(TableId(21)));
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
		});
		let (actor, _guard) = build_actor_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(ShapeId::Table(TableId(22)));
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
		});
		let (actor, _guard) = build_actor_with_read(Arc::new(AllPersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(ShapeId::Table(TableId(23)));
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
	fn sweep_invalidates_ephemeral_shape_in_read_tier() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		});
		let (actor, _guard) = build_actor_with_read(Arc::new(NonePersistent), CommitVersion(2), read.clone());
		let kind = EntryKind::Source(ShapeId::Table(TableId(24)));
		let key = ek("k");

		read.insert(key.clone(), CommitVersion(2), Some(val("stale")));
		write(&actor.commit, kind, &key, 2, "v2");

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(read.get(&key, CommitVersion(2)), VersionedGetResult::NotFound),
			"an ephemeral (persistent:false) shape must be invalidated in the read tier, never seeded"
		);
		assert!(
			matches!(
				actor.persistent.get(kind, key.as_ref(), CommitVersion(2)).unwrap(),
				VersionedGetResult::NotFound
			),
			"an ephemeral shape must not be persisted"
		);
	}

	#[test]
	fn sweep_seeds_accepted_keys_across_version_buckets() {
		let read = MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages: 16,
			..Default::default()
		});
		let (actor, _guard) = build_actor_with_read(Arc::new(AllPersistent), CommitVersion(4), read.clone());
		let kind = EntryKind::Source(ShapeId::Table(TableId(25)));
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
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(ShapeId::Table(TableId(12)));
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
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let kind = EntryKind::Source(ShapeId::Table(TableId(13)));
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
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(ShapeId::Table(TableId(101)));
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
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(2)));
		let row_kind = EntryKind::Source(ShapeId::Table(TableId(31)));
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

		let kind = EntryKind::Source(ShapeId::Table(TableId(32)));
		let batches = vec![(CommitVersion(1), HashMap::from([(kind, vec![(ek("k"), Some(val("v")))])]))];
		assert!(
			persistent.persist_sweep(batches).is_err(),
			"a shut-down persistent tier must refuse the sweep loudly so the buffer is not dropped"
		);
	}

	#[test]
	fn sweep_persists_all_kinds_and_versions_together() {
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(3)));
		let row_kind = EntryKind::Source(ShapeId::Table(TableId(33)));
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
		let (actor, _guard) = build_actor(Arc::new(AllPersistent), Some(CommitVersion(1)));
		let kind = EntryKind::Source(ShapeId::Table(TableId(102)));
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
}
