// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey, interface::store::EntryKind};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	traits::{Actor, Directive},
};
use reifydb_runtime::{
	actor::timers::TimerHandle,
	sync::{rwlock::RwLock, waiter::WaiterHandle},
};
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

	FlushPending {
		waiter: Arc<WaiterHandle>,
	},

	FlushAll {
		waiter: Arc<WaiterHandle>,
	},
}

#[allow(dead_code)]
pub struct FlushActorState {
	_timer_handle: Option<TimerHandle>,
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
		spawner.spawn_background("persistent-flush", actor).actor_ref().clone()
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
			EntryKind::Source(shape) => {
				self.persistence.get().map(|provider| provider.is_persistent(shape)).unwrap_or(true)
			}
			_ => true,
		}
	}

	fn sweep(&self, cutoff: CommitVersion) {
		let Some(entry_kinds) = self.list_evictable_kinds() else {
			return;
		};

		let mut persisted = 0usize;
		let mut dropped = 0usize;

		for kind in entry_kinds {
			let (to_persist, to_drop) = self.collect_evictable(kind, cutoff);
			if to_drop.is_empty() {
				continue;
			}

			if self.is_persistent_shape(kind) && !to_persist.is_empty() {
				let (count, persist_failed) = self.persist_evictable_batch(kind, to_persist);
				persisted += count;
				if persist_failed {
					continue;
				}
			}

			match self.invalidate_and_drop(kind, to_drop) {
				Some(count) => dropped += count,
				None => continue,
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
	fn persist_evictable_batch(
		&self,
		kind: EntryKind,
		to_persist: Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>,
	) -> (usize, bool) {
		let mut batch: HashMap<CommitVersion, TierBatch> = HashMap::new();
		for (key, version, value) in to_persist {
			batch.entry(version).or_default().entry(kind).or_default().push((key, value));
		}
		let mut persisted = 0usize;
		for (version, by_kind) in batch {
			let count: usize = by_kind.values().map(|v| v.len()).sum();
			if let Err(e) = self.persistent.set(version, by_kind) {
				error!(version = version.0, error = %e, "flush sweep: persist failed");
				return (persisted, true);
			}
			persisted += count;
		}
		(persisted, false)
	}

	#[inline]
	fn invalidate_and_drop(&self, kind: EntryKind, to_drop: Vec<(EncodedKey, CommitVersion)>) -> Option<usize> {
		let drop_count = to_drop.len();
		reifydb_assertions! {
			assert!(
				drop_count > 0,
				"sweep must only reach invalidate_and_drop with a non-empty drop set; an empty drop \
				 issues a no-op commit-buffer drop and lets the read-tier invalidation loop and the \
				 dropped counter run for zero work (kind={kind:?})"
			);
		}
		if let Some(read) = &self.read {
			for (key, _) in &to_drop {
				read.invalidate(key);
			}
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
			_timer_handle: Some(timer_handle),
		}
	}

	fn handle(&self, _state: &mut FlushActorState, msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
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
	fn sweep_invalidates_evicted_keys_in_the_read_tier() {
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
		assert!(matches!(read.get(&key, CommitVersion(2)), VersionedGetResult::Value { .. }));

		actor.sweep(CommitVersion(2));

		assert!(
			matches!(read.get(&key, CommitVersion(2)), VersionedGetResult::NotFound),
			"the read tier must be invalidated for keys evicted by the sweep"
		);
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
