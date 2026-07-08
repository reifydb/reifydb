// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::mem;
use std::sync::Arc;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_codec::key::encoded::EncodedKey;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	traits::{Actor, Directive},
};
use reifydb_runtime::{
	actor::timers::TimerHandle,
	sync::{mutex::Mutex, waiter::WaiterHandle},
};
use reifydb_value::value::{datetime::DateTime, duration::Duration};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_value::{reifydb_assertions, util::cowvec::CowVec};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use tracing::{debug, error};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::TierStorage;
use crate::{persistent::SinglePersistentTier, store::DirtyMap};

#[derive(Clone)]
pub enum FlushMessage {
	Tick(DateTime),
	Shutdown,
	FlushPending {
		waiter: Arc<WaiterHandle>,
	},
}

#[allow(dead_code)]
pub struct FlushActorState {
	_timer_handle: Option<TimerHandle>,
	flushing: bool,
}

#[allow(dead_code)]
pub struct FlushActor {
	dirty: Arc<Mutex<DirtyMap>>,
	persistent: SinglePersistentTier,
	flush_interval: Duration,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl FlushActor {
	pub fn new(dirty: Arc<Mutex<DirtyMap>>, persistent: SinglePersistentTier, flush_interval: Duration) -> Self {
		Self {
			dirty,
			persistent,
			flush_interval,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		dirty: Arc<Mutex<DirtyMap>>,
		persistent: SinglePersistentTier,
		flush_interval: Duration,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(dirty, persistent, flush_interval);
		spawner.spawn_coordination("single-persistent-flush", actor).actor_ref().clone()
	}

	fn drain(&self, state: &mut FlushActorState) {
		if !self.begin_flush(state) {
			return;
		}

		let drained = self.take_dirty();
		if drained.is_empty() {
			state.flushing = false;
			return;
		}

		reifydb_assertions! {
			let proceeding = state.flushing;
			assert!(
				proceeding,
				"flush_to_persistent reached with the reentrancy flag cleared, so a concurrent drain could \
				 take the dirty map and double-write the same rows to the persistent tier (flushing={proceeding})"
			);
		}

		self.flush_to_persistent(drained);
		state.flushing = false;
	}

	#[inline]
	fn begin_flush(&self, state: &mut FlushActorState) -> bool {
		if state.flushing {
			return false;
		}
		state.flushing = true;
		true
	}

	#[inline]
	fn take_dirty(&self) -> DirtyMap {
		let mut guard = self.dirty.lock();
		mem::take(&mut *guard)
	}

	#[inline]
	fn flush_to_persistent(&self, drained: DirtyMap) {
		let entries: Vec<(EncodedKey, Option<CowVec<u8>>)> = drained.into_iter().collect();
		let count = entries.len();
		if let Err(e) = self.persistent.set(entries) {
			error!(error = %e, "single persistent flush: set failed");
		} else {
			debug!(rows = count, "single persistent flush completed");
		}
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl Actor for FlushActor {
	type State = FlushActorState;
	type Message = FlushMessage;

	fn init(&self, ctx: &Context<FlushMessage>) -> FlushActorState {
		debug!("Single persistent flush actor started");
		let timer_handle = ctx.schedule_tick(self.flush_interval.to_std(), |nanos| {
			FlushMessage::Tick(DateTime::from_nanos(nanos))
		});
		FlushActorState {
			_timer_handle: Some(timer_handle),
			flushing: false,
		}
	}

	fn handle(&self, state: &mut FlushActorState, msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
		if ctx.is_cancelled() {
			self.drain(state);
			return Directive::Stop;
		}
		match msg {
			FlushMessage::Tick(_) => {
				self.drain(state);
			}
			FlushMessage::Shutdown => {
				debug!("Single persistent flush actor shutting down");
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
		debug!("Single persistent flush actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}
