// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	sync::waiter::WaiterHandle,
};
use reifydb_value::value::{datetime::DateTime, duration::Duration};
use tracing::debug;

use crate::{
	flush::block::flush_now,
	tier::{commit::CdcCommitBufferTier, persistent::CdcPersistentTier, read::CdcReadBufferTier},
};

const FLUSH_PENDING_TIMEOUT: Duration = Duration::from_seconds_const(5);

#[derive(Clone)]
pub enum FlushMessage {
	Tick(DateTime),
	Shutdown,
	FlushPending {
		waiter: Arc<WaiterHandle>,
	},
}

#[allow(dead_code)]
pub struct CdcFlushActorState {
	_timer_handle: Option<TimerHandle>,
}

pub struct CdcFlushActor {
	buffer: CdcCommitBufferTier,
	storage: CdcPersistentTier,
	read: Option<CdcReadBufferTier>,
	flush_interval: Duration,
}

impl CdcFlushActor {
	pub fn new(
		buffer: CdcCommitBufferTier,
		storage: CdcPersistentTier,
		read: Option<CdcReadBufferTier>,
		flush_interval: Duration,
	) -> Self {
		Self {
			buffer,
			storage,
			read,
			flush_interval,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		buffer: CdcCommitBufferTier,
		storage: CdcPersistentTier,
		read: Option<CdcReadBufferTier>,
		flush_interval: Duration,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(buffer, storage, read, flush_interval);
		spawner.spawn_coordination("cdc-persistent-flush", actor).actor_ref().clone()
	}

	fn drain(&self) {
		flush_now(&self.buffer, &self.storage, self.read.as_ref());
	}
}

pub fn flush_pending(actor_ref: &ActorRef<FlushMessage>) -> bool {
	let waiter = Arc::new(WaiterHandle::new());
	if actor_ref
		.send_blocking(FlushMessage::FlushPending {
			waiter: Arc::clone(&waiter),
		})
		.is_err()
	{
		return false;
	}
	waiter.wait_timeout(FLUSH_PENDING_TIMEOUT)
}

impl Actor for CdcFlushActor {
	type State = CdcFlushActorState;
	type Message = FlushMessage;

	fn init(&self, ctx: &Context<FlushMessage>) -> CdcFlushActorState {
		debug!("Cdc persistent flush actor started");
		let timer_handle = ctx.schedule_tick(self.flush_interval.to_std(), |nanos| {
			FlushMessage::Tick(DateTime::from_nanos(nanos))
		});
		CdcFlushActorState {
			_timer_handle: Some(timer_handle),
		}
	}

	fn handle(&self, _state: &mut CdcFlushActorState, msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
		if ctx.is_cancelled() {
			self.drain();
			if let FlushMessage::FlushPending {
				waiter,
			} = msg
			{
				waiter.notify();
			}
			return Directive::Stop;
		}
		match msg {
			FlushMessage::Tick(_) => {
				self.drain();
			}
			FlushMessage::Shutdown => {
				debug!("Cdc persistent flush actor shutting down");
				self.drain();
				return Directive::Stop;
			}
			FlushMessage::FlushPending {
				waiter,
			} => {
				self.drain();
				waiter.notify();
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Cdc persistent flush actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}
