// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	event::{
		EventBus,
		metric::{MultiCommittedEvent, MultiDrop},
	},
	interface::store::EntryKind,
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSystem},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	context::clock::{Clock, Instant},
};
use reifydb_type::util::cowvec::CowVec;
use tracing::{Span, debug, error, instrument};

use super::drop::find_keys_to_drop;
use crate::{buffer::storage::BufferStorage, tier::TierStorage};

#[derive(Debug, Clone)]
pub struct DropWorkerConfig {
	pub batch_size: usize,

	pub flush_interval: Duration,
}

impl Default for DropWorkerConfig {
	fn default() -> Self {
		Self {
			batch_size: 100,
			flush_interval: Duration::from_millis(50),
		}
	}
}

use reifydb_core::actors::drop::{DropMessage, DropRequest};

pub struct DropActor {
	storage: BufferStorage,
	event_bus: EventBus,
	config: DropWorkerConfig,
	clock: Clock,
}

pub struct DropActorState {
	pending_requests: Vec<DropRequest>,

	last_flush: Instant,

	_timer_handle: Option<TimerHandle>,

	flush_count: u64,
}

impl DropActor {
	pub fn new(config: DropWorkerConfig, storage: BufferStorage, event_bus: EventBus, clock: Clock) -> Self {
		Self {
			storage,
			event_bus,
			config,
			clock,
		}
	}

	pub fn spawn(
		system: &ActorSystem,
		config: DropWorkerConfig,
		storage: BufferStorage,
		event_bus: EventBus,
		clock: Clock,
	) -> ActorRef<DropMessage> {
		let actor = Self::new(config, storage, event_bus, clock);
		system.spawn_system("drop-worker", actor).actor_ref().clone()
	}

	fn maybe_flush(&self, state: &mut DropActorState) {
		if state.pending_requests.len() >= self.config.batch_size {
			self.flush(state);
		}
	}

	fn flush(&self, state: &mut DropActorState) {
		if state.pending_requests.is_empty() {
			return;
		}

		Self::process_batch(&self.storage, &mut state.pending_requests, &self.event_bus);
		state.last_flush = self.clock.instant();

		state.flush_count += 1;
		if state.flush_count.is_multiple_of(100) {
			self.storage.maintenance();
		}
	}

	#[instrument(name = "drop::process_batch", level = "debug", skip_all, fields(num_requests = requests.len(), total_dropped))]
	fn process_batch(storage: &BufferStorage, requests: &mut Vec<DropRequest>, event_bus: &EventBus) {
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>> = HashMap::new();

		let mut drops_with_stats = Vec::new();
		let mut max_pending_version = CommitVersion(0);

		for request in requests.drain(..) {
			let version_for_event = request.pending_version.unwrap_or(request.commit_version);
			if version_for_event > max_pending_version {
				max_pending_version = version_for_event;
			}

			match find_keys_to_drop(storage, request.table, request.key.as_ref(), request.pending_version) {
				Ok(entries_to_drop) => {
					for entry in entries_to_drop {
						drops_with_stats.push(MultiDrop {
							key: EncodedKey(request.key.clone()),
							value_bytes: entry.value_bytes,
						});

						batches.entry(request.table)
							.or_default()
							.push((entry.key, entry.version));
					}
				}
				Err(e) => {
					error!("Drop actor failed to find keys to drop: {}", e);
				}
			}
		}

		if !batches.is_empty()
			&& let Err(e) = storage.drop(batches)
		{
			error!("Drop actor failed to execute drops: {}", e);
		}

		let total_dropped = drops_with_stats.len();
		Span::current().record("total_dropped", total_dropped);

		event_bus.emit(MultiCommittedEvent::new(vec![], vec![], drops_with_stats, max_pending_version));
	}
}

impl Actor for DropActor {
	type State = DropActorState;
	type Message = DropMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		debug!("Drop actor started");

		let timer_handle = ctx.schedule_repeat(Duration::from_millis(10), DropMessage::Tick);

		DropActorState {
			pending_requests: Vec::with_capacity(self.config.batch_size),
			last_flush: self.clock.instant(),
			_timer_handle: Some(timer_handle),
			flush_count: 0,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			self.flush(state);
			return Directive::Stop;
		}

		match msg {
			DropMessage::Request(request) => {
				state.pending_requests.push(request);
				self.maybe_flush(state);
			}
			DropMessage::Batch(requests) => {
				state.pending_requests.extend(requests);
				self.maybe_flush(state);
			}
			DropMessage::Tick => {
				if !state.pending_requests.is_empty()
					&& state.last_flush.elapsed() >= self.config.flush_interval
				{
					self.flush(state);
				}
			}
			DropMessage::Shutdown => {
				debug!("Drop actor received shutdown signal");

				self.flush(state);
				return Directive::Stop;
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Drop actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096 * 16)
	}
}
