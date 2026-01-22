// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Background worker for deferred drop operations.
//!
//! This module provides an actor-based drop processing system that executes
//! version cleanup operations off the critical commit path.
//!
//! The actor model is platform-agnostic:
//! - **Native**: Runs on its own OS thread, processes messages from a channel
//! - **WASM**: Messages are processed inline (synchronously) when sent

use std::{collections::HashMap, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	event::{
		EventBus,
		metric::{StorageDrop, StorageStatsRecordedEvent},
	},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		timers::TimerHandle,
		traits::{Actor, ActorConfig, Flow},
		system::ActorSystem,
	},
	time::Instant,
};
use reifydb_type::util::cowvec::CowVec;
use tracing::{Span, debug, error, instrument};

use super::drop::find_keys_to_drop;
use crate::{
	hot::storage::HotStorage,
	tier::{EntryKind, TierStorage},
};

/// Configuration for the drop worker.
#[derive(Debug, Clone)]
pub struct DropWorkerConfig {
	/// How many drop requests to batch before executing.
	pub batch_size: usize,
	/// Maximum time to wait before flushing a partial batch.
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

/// A request to drop old versions of a key.
#[derive(Debug, Clone)]
pub struct DropRequest {
	/// The table containing the key.
	pub table: EntryKind,
	/// The logical key (without version suffix).
	pub key: CowVec<u8>,
	/// Drop versions below this threshold (if Some).
	pub up_to_version: Option<CommitVersion>,
	/// Keep this many most recent versions (if Some).
	pub keep_last_versions: Option<usize>,
	/// The commit version that created this drop request.
	pub commit_version: CommitVersion,
	/// A version being written in the same batch (to avoid race).
	pub pending_version: Option<CommitVersion>,
}

/// Messages for the drop actor.
#[derive(Clone)]
pub enum DropMessage {
	/// A single drop request to process.
	Request(DropRequest),
	/// A batch of drop requests to process.
	Batch(Vec<DropRequest>),
	/// Periodic tick for flushing batches.
	Tick,
	/// Shutdown the actor.
	Shutdown,
}

/// Actor that processes drop operations asynchronously.
///
/// Receives drop requests and batches them for efficient processing.
/// Uses the shared ActorRuntime, so it works in both native and WASM.
pub struct DropActor {
	storage: HotStorage,
	event_bus: EventBus,
	config: DropWorkerConfig,
}

/// State for the drop actor.
pub struct DropActorState {
	/// Pending requests waiting to be processed.
	pending_requests: Vec<DropRequest>,
	/// Last time we flushed the batch.
	last_flush: Instant,
	/// Handle to the periodic timer (for cleanup).
	_timer_handle: Option<TimerHandle>,
}

impl DropActor {
	/// Create a new drop actor.
	pub fn new(config: DropWorkerConfig, storage: HotStorage, event_bus: EventBus) -> Self {
		Self {
			storage,
			event_bus,
			config,
		}
	}

	/// Spawn a drop actor on the given system.
	///
	/// Returns the ActorRef for sending messages.
	pub fn spawn(
		system: &ActorSystem,
		config: DropWorkerConfig,
		storage: HotStorage,
		event_bus: EventBus,
	) -> ActorRef<DropMessage> {
		let actor = Self::new(config, storage, event_bus);
		system.spawn("drop-worker", actor).actor_ref().clone()
	}

	/// Maybe flush if batch is full.
	fn maybe_flush(&self, state: &mut DropActorState) {
		if state.pending_requests.len() >= self.config.batch_size {
			self.flush(state);
		}
	}

	/// Flush all pending requests.
	fn flush(&self, state: &mut DropActorState) {
		if state.pending_requests.is_empty() {
			return;
		}

		Self::process_batch(&self.storage, &mut state.pending_requests, &self.event_bus);
		state.last_flush = Instant::now();
	}

	#[instrument(name = "drop_actor::process_batch", level = "debug", skip_all, fields(num_requests = requests.len(), total_dropped))]
	fn process_batch(storage: &HotStorage, requests: &mut Vec<DropRequest>, event_bus: &EventBus) {
		// Collect all keys to drop, grouped by table: (key, version) pairs
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>> = HashMap::new();
		// Collect drop stats for metrics
		let mut drops_with_stats = Vec::new();
		let mut max_pending_version = CommitVersion(0);

		for request in requests.drain(..) {
			// Track highest version for event (prefer pending_version if set, otherwise use commit_version)
			let version_for_event = request.pending_version.unwrap_or(request.commit_version);
			if version_for_event > max_pending_version {
				max_pending_version = version_for_event;
			}

			match find_keys_to_drop(
				storage,
				request.table,
				request.key.as_ref(),
				request.up_to_version,
				request.keep_last_versions,
				request.pending_version,
			) {
				Ok(entries_to_drop) => {
					for entry in entries_to_drop {
						// Collect stats for metrics
						drops_with_stats.push(StorageDrop {
							key: EncodedKey(request.key.clone()),
							value_bytes: entry.value_bytes,
						});

						// Queue for physical deletion: (key, version) pair
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

		if !batches.is_empty() {
			if let Err(e) = storage.drop(batches) {
				error!("Drop actor failed to execute drops: {}", e);
			}
		}

		let total_dropped = drops_with_stats.len();
		Span::current().record("total_dropped", total_dropped);

		event_bus.emit(StorageStatsRecordedEvent::new(vec![], vec![], drops_with_stats, max_pending_version));
	}
}

impl Actor for DropActor {
	type State = DropActorState;
	type Message = DropMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		// Schedule periodic tick for flushing partial batches
		let timer_handle = ctx.schedule_repeat(Duration::from_millis(10), DropMessage::Tick);

		DropActorState {
			pending_requests: Vec::with_capacity(self.config.batch_size),
			last_flush: Instant::now(),
			_timer_handle: Some(timer_handle),
		}
	}

	fn pre_start(&self, _state: &mut Self::State, _ctx: &Context<Self::Message>) {
		debug!("Drop actor started");
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Flow {
		// Check for cancellation
		if ctx.is_cancelled() {
			// Flush remaining requests before stopping
			self.flush(state);
			return Flow::Stop;
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
				// Process any remaining requests before shutdown
				self.flush(state);
				return Flow::Stop;
			}
		}

		Flow::Continue
	}

	fn post_stop(&self, _state: &mut Self::State) {
		debug!("Drop actor stopped");
	}

	fn config(&self) -> ActorConfig {
		// Use a reasonable mailbox size for batched operations
		ActorConfig::new().mailbox_capacity(256)
	}
}
