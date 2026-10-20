// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use std::mem;
use std::sync::Arc;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::interface::store::SingleWalCheckpoint;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	traits::{Actor, Directive},
};
use reifydb_runtime::{actor::timers::TimerHandle, sync::mutex::Mutex};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_value::util::cowvec::CowVec;
use reifydb_value::value::{datetime::DateTime, duration::Duration};
use reifydb_wal::lsn::Lsn;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use tracing::debug;

use crate::{store::DirtyMap, tier::persistent::SinglePersistentTier};

#[derive(Clone)]
pub enum FlushMessage {
	Tick(DateTime),
	Shutdown,
}

#[allow(dead_code)]
pub struct FlushActorState {
	_timer_handle: Option<TimerHandle>,
}

#[allow(dead_code)]
pub struct Flusher {
	dirty: Arc<Mutex<DirtyMap>>,
	through: Arc<Mutex<Option<Lsn>>>,
	persistent: SinglePersistentTier,
	checkpoint: Arc<dyn SingleWalCheckpoint>,
	draining: Mutex<()>,
}

#[allow(dead_code)]
pub struct FlushActor {
	flusher: Arc<Flusher>,
	flush_interval: Duration,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl Flusher {
	pub fn new(
		dirty: Arc<Mutex<DirtyMap>>,
		through: Arc<Mutex<Option<Lsn>>>,
		persistent: SinglePersistentTier,
		checkpoint: Arc<dyn SingleWalCheckpoint>,
	) -> Self {
		Self {
			dirty,
			through,
			persistent,
			checkpoint,
			draining: Mutex::new(()),
		}
	}

	pub fn drain(&self) {
		let _draining = self.draining.lock();
		let (drained, through) = self.take_dirty();
		if drained.is_empty() {
			return;
		}
		self.flush_to_persistent(drained, through);
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
impl FlushActor {
	pub fn new(flusher: Arc<Flusher>, flush_interval: Duration) -> Self {
		Self {
			flusher,
			flush_interval,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		flusher: Arc<Flusher>,
		flush_interval: Duration,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(flusher, flush_interval);
		spawner.spawn_coordination("single-persistent-flush", actor).actor_ref().clone()
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
		}
	}

	fn handle(&self, _state: &mut FlushActorState, msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
		if ctx.is_cancelled() {
			self.flusher.drain();
			return Directive::Stop;
		}
		match msg {
			FlushMessage::Tick(_) => {
				self.flusher.drain();
			}
			FlushMessage::Shutdown => {
				debug!("Single persistent flush actor shutting down");
				self.flusher.drain();
				return Directive::Stop;
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
