// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::mem;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicBool};

use reifydb_core::interface::store::SingleWalCheckpoint;

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
use reifydb_wal::lsn::Lsn;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_value::{reifydb_assertions, util::cowvec::CowVec};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use tracing::debug;

use crate::{store::DirtyMap, tier::persistent::SinglePersistentTier};

#[derive(Clone)]
pub enum FlushMessage {
	Tick(DateTime),
	Shutdown,
	FlushPending {
		waiter: Arc<WaiterHandle>,
		flushed: Arc<AtomicBool>,
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
	through: Arc<Mutex<Option<Lsn>>>,
	persistent: SinglePersistentTier,
	flush_interval: Duration,
	checkpoint: Arc<dyn SingleWalCheckpoint>,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl FlushActor {
	pub fn new(
		dirty: Arc<Mutex<DirtyMap>>,
		through: Arc<Mutex<Option<Lsn>>>,
		persistent: SinglePersistentTier,
		flush_interval: Duration,
		checkpoint: Arc<dyn SingleWalCheckpoint>,
	) -> Self {
		Self {
			dirty,
			through,
			persistent,
			flush_interval,
			checkpoint,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		dirty: Arc<Mutex<DirtyMap>>,
		through: Arc<Mutex<Option<Lsn>>>,
		persistent: SinglePersistentTier,
		flush_interval: Duration,
		checkpoint: Arc<dyn SingleWalCheckpoint>,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(dirty, through, persistent, flush_interval, checkpoint);
		spawner.spawn_coordination("single-persistent-flush", actor).actor_ref().clone()
	}

	fn drain(&self, state: &mut FlushActorState) -> bool {
		if !self.begin_flush(state) {
			return false;
		}

		let (drained, through) = self.take_dirty();
		if drained.is_empty() {
			state.flushing = false;
			return true;
		}

		reifydb_assertions! {
			let proceeding = state.flushing;
			assert!(
				proceeding,
				"flush_to_persistent reached with the reentrancy flag cleared, so a concurrent drain could \
				 take the dirty map and double-write the same rows to the persistent tier (flushing={proceeding})"
			);
		}

		self.flush_to_persistent(drained, through);
		state.flushing = false;
		true
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
	fn take_dirty(&self) -> (DirtyMap, Option<Lsn>) {
		let mut guard = self.dirty.lock();
		let through = self.through.lock().take();
		(mem::take(&mut *guard), through)
	}

	#[inline]
	fn flush_to_persistent(&self, drained: DirtyMap, through: Option<Lsn>) {
		let entries: Vec<(EncodedKey, Option<CowVec<u8>>)> =
			drained.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
		let count = entries.len();
		if let Some(through) = through {
			self.checkpoint.make_durable(through).unwrap_or_else(|err| {
				panic!(
					"single flush could not make the commit log durable through lsn {}: {err}",
					through.as_u64()
				)
			});
		}
		self.persistent.persist(entries, through).unwrap_or_else(|err| {
			panic!("single flush could not persist {count} rows through lsn {through:?}: {err}")
		});
		if let Some(through) = through {
			self.checkpoint.saved_through(through).unwrap_or_else(|err| {
				panic!(
					"single flush saved through lsn {} but could not move its commit log floor: {err}",
					through.as_u64()
				)
			});
		}
		debug!(rows = count, "single persistent flush completed");
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
			let _ = self.drain(state);
			return Directive::Stop;
		}
		match msg {
			FlushMessage::Tick(_) => {
				let _ = self.drain(state);
			}
			FlushMessage::Shutdown => {
				debug!("Single persistent flush actor shutting down");
				let _ = self.drain(state);
				return Directive::Stop;
			}
			FlushMessage::FlushPending {
				waiter,
				flushed,
			} => {
				flushed.store(self.drain(state), Ordering::Release);
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
