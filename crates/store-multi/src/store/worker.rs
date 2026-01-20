// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Background worker for deferred drop operations.
//!
//! This module provides an asynchronous drop processing worker that executes
//! version cleanup operations off the critical commit path.

use std::time::Duration;

#[cfg(feature = "native")]
use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
};

#[cfg(feature = "native")]
use crossbeam_channel::{Receiver, Sender, bounded};

#[cfg(feature = "native")]
use tracing::{debug, error, trace};

#[cfg(feature = "native")]
use reifydb_runtime::time::native::Instant;

use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
};

#[cfg(feature = "native")]
use reifydb_core::{
	encoded::key::EncodedKey,
	event::metric::{StorageDrop, StorageStatsRecordedEvent},
};

use reifydb_type::util::cowvec::CowVec;

#[cfg(feature = "native")]
use super::drop::find_keys_to_drop;
#[cfg(feature = "native")]
use crate::tier::TierStorage;

use crate::{
	hot::storage::HotStorage,
	tier::EntryKind,
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
#[cfg(feature = "native")]
enum DropMessage {
	/// A drop request to process.
	Request(DropRequest),
	/// Shutdown the worker.
	Shutdown,
}

/// Background worker for processing drop operations (native with threads).
#[cfg(feature = "native")]
pub struct DropWorker {
	sender: Sender<DropMessage>,
	running: Arc<AtomicBool>,
	worker: Option<JoinHandle<()>>,
}

/// No-op drop worker for WASM (single-threaded).
#[cfg(feature = "wasm")]
pub struct DropWorker {
	_config: DropWorkerConfig,
}

#[cfg(feature = "native")]
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

							// Flush if batch is full
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

#[cfg(feature = "native")]
impl Drop for DropWorker {
	fn drop(&mut self) {
		self.stop();
	}
}

// ===== WASM Implementation =====

#[cfg(feature = "wasm")]
impl DropWorker {
	/// Create a no-op drop worker for WASM (no background threads).
	pub fn new(config: DropWorkerConfig, _storage: HotStorage, _event_bus: EventBus) -> Self {
		Self { _config: config }
	}

	/// Queue a drop request (no-op in WASM).
	#[inline]
	pub fn queue_drop(
		&self,
		_table: crate::tier::EntryKind,
		_key: CowVec<u8>,
		_up_to_version: Option<CommitVersion>,
		_keep_last_versions: Option<usize>,
		_pending_version: Option<CommitVersion>,
	) {
		// No-op in WASM - drops are not processed in background
	}

	/// Stop the worker (no-op in WASM).
	pub fn stop(&mut self) {
		// No-op in WASM
	}
}
