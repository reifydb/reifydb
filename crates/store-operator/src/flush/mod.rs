// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod tests;

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

use crate::tier::{
	commit::{
		OperatorCommitBuffer,
		batch::{DropMarker, FlushBatch},
	},
	persistent::OperatorPersistentTier,
	point::OperatorPointTier,
	range::OperatorRangeTier,
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
pub struct OperatorFlushActorState {
	_timer_handle: Option<TimerHandle>,
}

pub struct OperatorFlushActor {
	buffer: OperatorCommitBuffer,
	storage: OperatorPersistentTier,
	point: Option<OperatorPointTier>,
	range: Option<OperatorRangeTier>,
	flush_interval: Duration,
}

impl OperatorFlushActor {
	pub fn new(
		buffer: OperatorCommitBuffer,
		storage: OperatorPersistentTier,
		point: Option<OperatorPointTier>,
		range: Option<OperatorRangeTier>,
		flush_interval: Duration,
	) -> Self {
		Self {
			buffer,
			storage,
			point,
			range,
			flush_interval,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn spawn(
		spawner: &ActorSpawner,
		buffer: OperatorCommitBuffer,
		storage: OperatorPersistentTier,
		point: Option<OperatorPointTier>,
		range: Option<OperatorRangeTier>,
		flush_interval: Duration,
	) -> ActorRef<FlushMessage> {
		let actor = Self::new(buffer, storage, point, range, flush_interval);
		spawner.spawn_coordination("operator-persistent-flush", actor).actor_ref().clone()
	}

	#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
	pub fn spawn(
		_spawner: &ActorSpawner,
		_buffer: OperatorCommitBuffer,
		storage: OperatorPersistentTier,
		_point: Option<OperatorPointTier>,
		_range: Option<OperatorRangeTier>,
		_flush_interval: Duration,
	) -> ActorRef<FlushMessage> {
		match storage {}
	}

	fn drain(&self) {
		flush_now(&self.buffer, &self.storage, self.point.as_ref(), self.range.as_ref());
	}
}

pub fn flush_now(
	buffer: &OperatorCommitBuffer,
	storage: &OperatorPersistentTier,
	point: Option<&OperatorPointTier>,
	range: Option<&OperatorRangeTier>,
) {
	let _flushing = buffer.flush_guard();
	while let Some(batch) = buffer.take_for_flush() {
		storage.flush_batch(&batch);
		invalidate_flushed(point, range, &batch);
		buffer.complete_flush();
	}
}

fn invalidate_flushed(point: Option<&OperatorPointTier>, range: Option<&OperatorRangeTier>, batch: &FlushBatch) {
	for marker in &batch.drops {
		match marker {
			DropMarker::OperatorState(operator) => {
				if let Some(range) = range {
					range.invalidate_operator(*operator);
				}
				if let Some(point) = point {
					point.invalidate_operator(*operator);
				}
			}
			DropMarker::AnchorsOperator(_) | DropMarker::AnchorsGroup(_, _) => {}
		}
	}
	for ((operator, key), row) in &batch.state {
		match row {
			Some(row) => {
				if let Some(range) = range {
					range.overwrite(*operator, key.clone(), row.clone());
				}
				if let Some(point) = point {
					point.overwrite(*operator, key.clone(), row.clone());
				}
			}
			None => {
				if let Some(range) = range {
					range.invalidate(*operator, key);
				}
				if let Some(point) = point {
					point.invalidate(*operator, key);
				}
			}
		}
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

impl Actor for OperatorFlushActor {
	type State = OperatorFlushActorState;
	type Message = FlushMessage;

	fn init(&self, ctx: &Context<FlushMessage>) -> OperatorFlushActorState {
		debug!("Operator persistent flush actor started");
		let timer_handle = ctx.schedule_tick(self.flush_interval.to_std(), |nanos| {
			FlushMessage::Tick(DateTime::from_nanos(nanos))
		});
		OperatorFlushActorState {
			_timer_handle: Some(timer_handle),
		}
	}

	fn handle(
		&self,
		_state: &mut OperatorFlushActorState,
		msg: FlushMessage,
		ctx: &Context<FlushMessage>,
	) -> Directive {
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
				debug!("Operator persistent flush actor shutting down");
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
		debug!("Operator persistent flush actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}
