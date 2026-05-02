// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::mem;
use std::{collections::HashMap, sync::Arc, time::Duration};

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey, interface::store::EntryKind};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSystem},
	traits::{Actor, Directive},
};
use reifydb_runtime::{actor::timers::TimerHandle, sync::waiter::WaiterHandle};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_type::util::cowvec::CowVec;
use reifydb_type::value::datetime::DateTime;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use tracing::{debug, error, warn};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::{TierBatch, TierStorage};
use crate::{hot::storage::HotStorage, warm::WarmStorage};

#[derive(Clone)]
pub enum FlushMessage {
	/// One commit's worth of dirty keys observed via `MultiCommittedEvent`.
	///
	/// All keys in `sets` and `tombstones` were committed at `version`.
	Dirty {
		version: CommitVersion,
		sets: HashMap<EntryKind, Vec<EncodedKey>>,
		tombstones: HashMap<EntryKind, Vec<EncodedKey>>,
	},
	Tick(DateTime),
	Shutdown,
	/// Drain the actor's accumulated `pending` map synchronously and notify
	/// the waiter when finished. Same code path as the periodic `Tick`,
	/// just triggered on demand.
	FlushPending {
		waiter: Arc<WaiterHandle>,
	},
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
struct PendingEntry {
	version: CommitVersion,
	is_tombstone: bool,
}

#[allow(dead_code)]
pub struct FlushActorState {
	_timer_handle: Option<TimerHandle>,
	pending: HashMap<EntryKind, HashMap<EncodedKey, PendingEntry>>,
	flushing: bool,
}

/// Periodic warm-tier flush actor.
///
/// Subscribes (via `FlushEventListener`) to `MultiCommittedEvent` and buffers
/// dirty keys per `EntryKind`. On each tick, drains the buffer: groups keys by
/// the commit version that touched them, reads the corresponding values from
/// hot at that version, and writes the batch to warm via
/// `TierStorage::set(version, batches)`.
///
/// Phase 1: warm is a passive mirror. No eviction from hot. Drops are not
/// flushed (drop is a hot-only concept; warm has no historical chain).
#[allow(dead_code)]
pub struct FlushActor {
	hot: HotStorage,
	warm: WarmStorage,
	flush_interval: Duration,
}

// On wasm32 / no-sqlite, `WarmStorage` is uninhabited so `FlushActor` itself
// is uninhabited; the methods below would all be unreachable code. The
// FlushActor type definition stays so `FlushMessage` and the `Option<ActorRef<...>>`
// field in `StandardMultiStoreInner` remain unconditional, but the construction
// and Actor-trait impls are sqlite-only.
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl FlushActor {
	pub fn new(hot: HotStorage, warm: WarmStorage, flush_interval: Duration) -> Self {
		Self {
			hot,
			warm,
			flush_interval,
		}
	}

	pub fn spawn(
		system: &ActorSystem,
		hot: HotStorage,
		warm: WarmStorage,
		flush_interval: Duration,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(hot, warm, flush_interval);
		system.spawn_system("warm-flush", actor).actor_ref().clone()
	}

	fn merge_dirty(
		&self,
		state: &mut FlushActorState,
		version: CommitVersion,
		sets: HashMap<EntryKind, Vec<EncodedKey>>,
		tombstones: HashMap<EntryKind, Vec<EncodedKey>>,
	) {
		for (kind, keys) in sets {
			let slot = state.pending.entry(kind).or_default();
			for key in keys {
				upsert_pending(slot, key, version, false);
			}
		}
		for (kind, keys) in tombstones {
			let slot = state.pending.entry(kind).or_default();
			for key in keys {
				upsert_pending(slot, key, version, true);
			}
		}
	}

	fn drain(&self, state: &mut FlushActorState) {
		if state.flushing || state.pending.is_empty() {
			return;
		}
		state.flushing = true;

		let pending = mem::take(&mut state.pending);

		// Group by the version each key was committed at, since
		// `TierStorage::set(version, batches)` writes a single version per call.
		let mut by_version: HashMap<CommitVersion, TierBatch> = HashMap::new();

		for (kind, keys_map) in pending {
			for (key, entry) in keys_map {
				let value = if entry.is_tombstone {
					None
				} else {
					match self.hot.get(kind, key.as_ref(), entry.version) {
						Ok(Some(v)) => Some(v),
						Ok(None) => {
							// Hot dropped the key before we got here, or the
							// commit applied a tombstone via Remove. Skip:
							// nothing to mirror.
							continue;
						}
						Err(e) => {
							warn!(?kind, error = %e, "warm flush: hot read failed");
							continue;
						}
					}
				};

				by_version
					.entry(entry.version)
					.or_default()
					.entry(kind)
					.or_default()
					.push((CowVec::new(key.0.to_vec()), value));
			}
		}

		let mut total = 0usize;
		for (version, batch) in by_version {
			let count: usize = batch.values().map(|v| v.len()).sum();
			if let Err(e) = self.warm.set(version, batch) {
				error!(version = version.0, error = %e, "warm flush: set failed");
			} else {
				total += count;
			}
		}

		if total > 0 {
			debug!(rows = total, "warm flush completed");
		}

		state.flushing = false;
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
fn upsert_pending(
	slot: &mut HashMap<EncodedKey, PendingEntry>,
	key: EncodedKey,
	version: CommitVersion,
	is_tombstone: bool,
) {
	match slot.get_mut(&key) {
		Some(existing) if existing.version >= version => {
			// Existing pending entry is newer or equal; keep it.
		}
		Some(existing) => {
			existing.version = version;
			existing.is_tombstone = is_tombstone;
		}
		None => {
			slot.insert(
				key,
				PendingEntry {
					version,
					is_tombstone,
				},
			);
		}
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl Actor for FlushActor {
	type State = FlushActorState;
	type Message = FlushMessage;

	fn init(&self, ctx: &Context<FlushMessage>) -> FlushActorState {
		debug!("Warm flush actor started");
		let timer_handle =
			ctx.schedule_tick(self.flush_interval, |nanos| FlushMessage::Tick(DateTime::from_nanos(nanos)));
		FlushActorState {
			_timer_handle: Some(timer_handle),
			pending: HashMap::new(),
			flushing: false,
		}
	}

	fn handle(&self, state: &mut FlushActorState, msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
		if ctx.is_cancelled() {
			self.drain(state);
			return Directive::Stop;
		}
		match msg {
			FlushMessage::Dirty {
				version,
				sets,
				tombstones,
			} => {
				self.merge_dirty(state, version, sets, tombstones);
			}
			FlushMessage::Tick(_) => {
				self.drain(state);
			}
			FlushMessage::Shutdown => {
				debug!("Warm flush actor shutting down");
				self.drain(state);
				return Directive::Stop;
			}
			FlushMessage::FlushPending {
				waiter,
			} => {
				self.drain(state);
				waiter.notify();
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Warm flush actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}
