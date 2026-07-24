// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::compaction::CompactionRequest,
	common::CommitVersion,
	event::{
		EventBus,
		metric::{MultiCommittedEvent, MultiCompaction},
	},
	interface::store::EntryKind,
	lifecycle::progress::Progress,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::duration::Duration;
use tracing::{Span, error, instrument};

use super::compaction::find_superseded_versions;
use crate::tier::{TierStorage, commit::buffer::MultiCommitBufferTier};

#[derive(Debug, Clone)]
pub struct CompactionWorkerConfig {
	pub batch_size: usize,

	pub flush_interval: Duration,
}

impl Default for CompactionWorkerConfig {
	fn default() -> Self {
		Self {
			batch_size: 100,
			flush_interval: Duration::from_milliseconds(50).unwrap(),
		}
	}
}

#[derive(Default)]
struct CompactionEngineState {
	drain_count: u64,
}

pub struct CompactionEngine {
	storage: MultiCommitBufferTier,
	event_bus: EventBus,
	config: CompactionWorkerConfig,
	intake: Mutex<Vec<CompactionRequest>>,
	drain_lock: Mutex<CompactionEngineState>,
}

impl CompactionEngine {
	pub fn new(config: CompactionWorkerConfig, storage: MultiCommitBufferTier, event_bus: EventBus) -> Self {
		Self {
			storage,
			event_bus,
			config,
			intake: Mutex::new(Vec::new()),
			drain_lock: Mutex::new(CompactionEngineState::default()),
		}
	}

	pub fn drain_budget(&self) -> usize {
		self.config.batch_size
	}

	pub fn flush_interval(&self) -> Duration {
		self.config.flush_interval
	}

	pub fn enqueue(&self, requests: Vec<CompactionRequest>) {
		if requests.is_empty() {
			return;
		}
		self.intake.lock().extend(requests);
	}

	pub fn drain_slice(&self, budget: usize) -> Progress {
		let mut state = self.drain_lock.lock();
		self.drain_once(&mut state, budget)
	}

	pub fn drain_to_exhaustion(&self) {
		let mut state = self.drain_lock.lock();
		loop {
			let progress = self.drain_once(&mut state, usize::MAX);
			if progress.is_exhausted() && self.intake.lock().is_empty() {
				break;
			}
		}
	}

	fn drain_once(&self, state: &mut CompactionEngineState, budget: usize) -> Progress {
		let mut taken: Vec<CompactionRequest> = {
			let mut queue = self.intake.lock();
			let take = queue.len().min(budget);
			queue.drain(..take).collect()
		};
		if !taken.is_empty() {
			Self::process_batch(&self.storage, &mut taken, &self.event_bus);
		}

		state.drain_count += 1;
		if state.drain_count.is_multiple_of(100) {
			self.storage.maintenance();
		}

		let intake_more = !self.intake.lock().is_empty();
		if intake_more {
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}

	#[instrument(name = "compaction::process_batch", level = "debug", skip_all, fields(num_requests = requests.len(), total_dropped))]
	fn process_batch(storage: &MultiCommitBufferTier, requests: &mut Vec<CompactionRequest>, event_bus: &EventBus) {
		let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();

		let mut drops_with_stats = Vec::new();
		let mut max_pending_version = CommitVersion(0);

		for request in requests.drain(..) {
			let version_for_event = request.pending_version.unwrap_or(request.commit_version);
			if version_for_event > max_pending_version {
				max_pending_version = version_for_event;
			}

			match find_superseded_versions(
				storage,
				request.table,
				request.key.as_ref(),
				request.pending_version,
			) {
				Ok(entries_to_drop) => {
					for entry in entries_to_drop {
						drops_with_stats.push(MultiCompaction {
							key: request.key.clone(),
							value_bytes: entry.value_bytes,
						});
						batches.entry(request.table)
							.or_default()
							.push((entry.key, entry.version));
					}
				}
				Err(e) => {
					error!("Drop engine failed to find keys to drop: {}", e);
				}
			}
		}

		if !batches.is_empty()
			&& let Err(e) = storage.compact(batches)
		{
			error!("Drop engine failed to execute drops: {}", e);
		}

		let total_dropped = drops_with_stats.len();
		Span::current().record("total_dropped", total_dropped);

		event_bus.emit(MultiCommittedEvent::new(vec![], vec![], drops_with_stats, max_pending_version));
	}
}
