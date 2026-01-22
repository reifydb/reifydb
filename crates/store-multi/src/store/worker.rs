// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Background worker for deferred drop operations.
//!
//! This module provides an asynchronous drop processing worker that executes
//! version cleanup operations off the critical commit path.

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, unbounded};
use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	event::{
		EventBus,
		metric::{StorageDrop, StorageStatsRecordedEvent},
	},
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

/// Control messages for the drop worker.
pub enum DropMessage {
	/// A drop request to process.
	Request(DropRequest),
	/// A batch of drop requests to process.
	Batch(Vec<DropRequest>),
	/// Shutdown the worker.
	Shutdown,
}

/// Background worker for processing drop operations.
pub struct DropWorker {
	sender: Sender<DropMessage>,
	running: Arc<AtomicBool>,
	worker: Option<JoinHandle<()>>,
}

impl DropWorker {
	/// Create and start a new drop worker.
	pub fn new(config: DropWorkerConfig, storage: HotStorage, event_bus: EventBus) -> Self {
		let (sender, receiver) = unbounded();
		let running = Arc::new(AtomicBool::new(true));

		let worker_running = Arc::clone(&running);
		let worker = thread::Builder::new()
			.name("store-worker".to_string())
			.spawn(move || {
				Self::worker_loop(receiver, storage, config, worker_running, event_bus);
			})
			.expect("Failed to spawn store worker thread");

		Self {
			sender,
			running,
			worker: Some(worker),
		}
	}

	/// Get a clone of the sender for queueing drop requests.
	pub fn sender(&self) -> Sender<DropMessage> {
		self.sender.clone()
	}

	/// Stop the worker gracefully.
	pub fn stop(&mut self) {
		if !self.running.swap(false, Ordering::AcqRel) {
			return;
		}

		// Send shutdown signal
		let _ = self.sender.send(DropMessage::Shutdown);

		// Wait for worker to finish
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}

	fn worker_loop(
		receiver: Receiver<DropMessage>,
		storage: HotStorage,
		config: DropWorkerConfig,
		running: Arc<AtomicBool>,
		event_bus: EventBus,
	) {
		debug!("Drop worker started");

		let mut pending_requests: Vec<DropRequest> = Vec::with_capacity(config.batch_size);
		let mut last_flush = Instant::now();

		while running.load(Ordering::Acquire) {
			// Use timeout to allow periodic flush checks
			match receiver.recv_timeout(Duration::from_millis(10)) {
				Ok(message) => {
					match message {
						DropMessage::Request(request) => {
							pending_requests.push(request);

							if pending_requests.len() >= config.batch_size {
								Self::process_batch(
									&storage,
									&mut pending_requests,
									&event_bus,
								);
								last_flush = Instant::now();
							}
						}
						DropMessage::Batch(requests) => {
							pending_requests.extend(requests);

							if pending_requests.len() >= config.batch_size {
								Self::process_batch(
									&storage,
									&mut pending_requests,
									&event_bus,
								);
								last_flush = Instant::now();
							}
						}
						DropMessage::Shutdown => {
							debug!("Drop worker received shutdown signal");
							// Process any remaining requests before shutdown
							if !pending_requests.is_empty() {
								Self::process_batch(
									&storage,
									&mut pending_requests,
									&event_bus,
								);
							}
							break;
						}
					}
				}
				Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
					// Check for periodic flush
				}
				Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
					debug!("Drop worker channel disconnected");
					break;
				}
			}

			// Periodic flush
			if !pending_requests.is_empty() && last_flush.elapsed() >= config.flush_interval {
				Self::process_batch(&storage, &mut pending_requests, &event_bus);
				last_flush = Instant::now();
			}
		}

		debug!("Drop worker stopped");
	}

	#[instrument(name = "drop_worker::process_batch", level = "debug", skip_all, fields(num_requests = requests.len(), total_dropped))]
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
						batches.entry(request.table).or_default().push((entry.key, entry.version));
					}
				}
				Err(e) => {
					error!("Drop worker failed to find keys to drop: {}", e);
				}
			}
		}

		if !batches.is_empty() {
			if let Err(e) = storage.drop(batches) {
				error!("Drop worker failed to execute drops: {}", e);
			}
		}

		let total_dropped = drops_with_stats.len();
		Span::current().record("total_dropped", total_dropped);

		event_bus.emit(StorageStatsRecordedEvent::new(vec![], vec![], drops_with_stats, max_pending_version));
	}
}

impl Drop for DropWorker {
	fn drop(&mut self) {
		self.stop();
	}
}
