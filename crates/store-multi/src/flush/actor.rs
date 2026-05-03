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
use crate::{buffer::storage::BufferStorage, persistent::PersistentStorage};

#[derive(Clone)]
pub enum FlushMessage {
	Dirty {
		version: CommitVersion,
		sets: HashMap<EntryKind, Vec<EncodedKey>>,
		tombstones: HashMap<EntryKind, Vec<EncodedKey>>,
	},
	Tick(DateTime),
	Shutdown,

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

#[allow(dead_code)]
pub struct FlushActor {
	buffer: BufferStorage,
	persistent: PersistentStorage,
	flush_interval: Duration,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl FlushActor {
	pub fn new(buffer: BufferStorage, persistent: PersistentStorage, flush_interval: Duration) -> Self {
		Self {
			buffer,
			persistent,
			flush_interval,
		}
	}

	pub fn spawn(
		system: &ActorSystem,
		buffer: BufferStorage,
		persistent: PersistentStorage,
		flush_interval: Duration,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(buffer, persistent, flush_interval);
		system.spawn_system("persistent-flush", actor).actor_ref().clone()
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

		let mut by_version: HashMap<CommitVersion, TierBatch> = HashMap::new();

		for (kind, keys_map) in pending {
			for (key, entry) in keys_map {
				let value = if entry.is_tombstone {
					None
				} else {
					match self.buffer.get(kind, key.as_ref(), entry.version) {
						Ok(Some(v)) => Some(v),
						Ok(None) => {
							continue;
						}
						Err(e) => {
							warn!(?kind, error = %e, "persistent flush: buffer read failed");
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
			if let Err(e) = self.persistent.set(version, batch) {
				error!(version = version.0, error = %e, "persistent flush: set failed");
			} else {
				total += count;
			}
		}

		if total > 0 {
			debug!(rows = total, "persistent flush completed");
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
		Some(existing) if existing.version >= version => {}
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
		debug!("Persistent flush actor started");
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
				debug!("Persistent flush actor shutting down");
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
		debug!("Persistent flush actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}
