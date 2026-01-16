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
	time::Duration,
};

use crossbeam_channel::{Receiver, Sender, bounded};
use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	event::{
		EventBus,
		metric::{StorageDrop, StorageStatsRecordedEvent},
	},
};
use reifydb_type::util::cowvec::CowVec;
use tracing::{debug, error, trace};

use super::drop::find_keys_to_drop;
use crate::{
	hot::storage::HotStorage,
	tier::{EntryKind, TierStorage},
};

/// Configuration for the drop worker.
#[derive(Debug, Clone)]
pub struct DropWorkerConfig {
	/// Maximum number of pending drop requests in the channel.
	pub channel_capacity: usize,
	/// How many drop requests to batch before executing.
	pub batch_size: usize,
	/// Maximum time to wait before flushing a partial batch.
	pub flush_interval: Duration,
}

impl Default for DropWorkerConfig {
	fn default() -> Self {
		Self {
			channel_capacity: 10_000,
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
	/// A version being written in the same batch (to avoid race).
	pub pending_version: Option<CommitVersion>,
}

/// Control messages for the drop worker.
enum DropMessage {
	/// A drop request to process.
	Request(DropRequest),
	/// Flush pending requests and acknowledge completion.
	Flush(Sender<()>),
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
		let (sender, receiver) = bounded(config.channel_capacity);
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

	/// Queue a drop request for processing.
	#[inline]
	pub fn queue_drop(
		&self,
		table: EntryKind,
		key: CowVec<u8>,
		up_to_version: Option<CommitVersion>,
		keep_last_versions: Option<usize>,
		pending_version: Option<CommitVersion>,
	) {
		let request = DropRequest {
			table,
			key,
			up_to_version,
			keep_last_versions,
			pending_version,
		};
		// Fire and forget - if channel is full, drop will happen on next commit
		let _ = self.sender.try_send(DropMessage::Request(request));
	}

	/// Force flush all pending drop requests.
	///
	/// This blocks until all pending requests have been processed.
	/// Useful for tests to ensure stats are up-to-date before reading.
	pub fn flush(&self) {
		let (ack_sender, ack_receiver) = bounded(1);
		if self.sender.send(DropMessage::Flush(ack_sender)).is_ok() {
			// Wait for acknowledgment (ignore timeout/disconnect)
			let _ = ack_receiver.recv();
		}
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
		let mut last_flush = std::time::Instant::now();

		while running.load(Ordering::Acquire) {
			// Use timeout to allow periodic flush checks
			match receiver.recv_timeout(Duration::from_millis(10)) {
				Ok(message) => {
					match message {
						DropMessage::Request(request) => {
							pending_requests.push(request);

							// Flush if batch is full
							if pending_requests.len() >= config.batch_size {
								Self::process_batch(
									&storage,
									&mut pending_requests,
									&event_bus,
								);
								last_flush = std::time::Instant::now();
							}
						}
						DropMessage::Flush(ack) => {
							// Process any pending requests before acknowledging
							if !pending_requests.is_empty() {
								Self::process_batch(
									&storage,
									&mut pending_requests,
									&event_bus,
								);
								last_flush = std::time::Instant::now();
							}
							// Acknowledge completion
							let _ = ack.send(());
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
				last_flush = std::time::Instant::now();
			}
		}

		debug!("Drop worker stopped");
	}

	fn process_batch(storage: &HotStorage, requests: &mut Vec<DropRequest>, event_bus: &EventBus) {
		trace!("Drop worker processing {} requests", requests.len());

		// Collect all entries to delete, grouped by table
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>> = HashMap::new();
		// Collect drop stats for metrics
		let mut drops_with_stats = Vec::new();
		let mut max_pending_version = CommitVersion(0);

		for request in requests.drain(..) {
			// Track highest version for event
			if let Some(pv) = request.pending_version {
				if pv > max_pending_version {
					max_pending_version = pv;
				}
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

						// Queue for deletion (None value = delete)
						batches.entry(request.table)
							.or_default()
							.push((entry.versioned_key, None));
					}
				}
				Err(e) => {
					error!("Drop worker failed to find keys to drop: {}", e);
				}
			}
		}

		// Execute all deletes in a single batch write
		if !batches.is_empty() {
			if let Err(e) = storage.set(batches) {
				error!("Drop worker failed to execute deletes: {}", e);
			}
		}

		// Emit stats event for metrics tracking
		if !drops_with_stats.is_empty() {
			event_bus.emit(StorageStatsRecordedEvent {
				writes: vec![],
				deletes: vec![],
				drops: drops_with_stats,
				version: max_pending_version,
			});
		}
	}
}

impl Drop for DropWorker {
	fn drop(&mut self) {
		self.stop();
	}
}
