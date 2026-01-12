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
use reifydb_core::CommitVersion;
use reifydb_type::CowVec;
use tracing::{debug, error, trace};

use super::drop::find_keys_to_drop;
use crate::{
	hot::HotStorage,
	stats::Tier,
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
	/// The commit version that triggered this drop (for stats tracking).
	pub version: CommitVersion,
}

/// Control messages for the drop worker.
#[derive(Debug)]
enum DropMessage {
	/// A drop request to process.
	Request(DropRequest),
	/// Shutdown the worker.
	Shutdown,
}

/// Callback for stats tracking when drops are executed.
pub trait DropStatsCallback: Send + 'static {
	fn record_drop(
		&self,
		tier: Tier,
		key: &[u8],
		versioned_key_bytes: u64,
		value_bytes: u64,
		version: CommitVersion,
	);
}

/// Background worker for processing drop operations.
pub struct DropWorker {
	sender: Sender<DropMessage>,
	running: Arc<AtomicBool>,
	worker: Option<JoinHandle<()>>,
}

impl DropWorker {
	/// Create and start a new drop worker.
	pub fn new<C: DropStatsCallback>(
		config: DropWorkerConfig,
		storage: HotStorage,
		stats_callback: C,
	) -> Self {
		let (sender, receiver) = bounded(config.channel_capacity);
		let running = Arc::new(AtomicBool::new(true));

		let worker_running = Arc::clone(&running);
		let worker = thread::Builder::new()
			.name("store-worker".to_string())
			.spawn(move || {
				Self::worker_loop(receiver, storage, stats_callback, config, worker_running);
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
		version: CommitVersion,
	) {
		let request = DropRequest {
			table,
			key,
			up_to_version,
			keep_last_versions,
			pending_version,
			version,
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

	fn worker_loop<C: DropStatsCallback>(
		receiver: Receiver<DropMessage>,
		storage: HotStorage,
		stats_callback: C,
		config: DropWorkerConfig,
		running: Arc<AtomicBool>,
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
								Self::process_batch(&storage, &stats_callback, &mut pending_requests);
								last_flush = std::time::Instant::now();
							}
						}
						DropMessage::Shutdown => {
							debug!("Drop worker received shutdown signal");
							// Process any remaining requests before shutdown
							if !pending_requests.is_empty() {
								Self::process_batch(&storage, &stats_callback, &mut pending_requests);
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
				Self::process_batch(&storage, &stats_callback, &mut pending_requests);
				last_flush = std::time::Instant::now();
			}
		}

		debug!("Drop worker stopped");
	}

	fn process_batch<C: DropStatsCallback>(
		storage: &HotStorage,
		stats_callback: &C,
		requests: &mut Vec<DropRequest>,
	) {
		trace!("Drop worker processing {} requests", requests.len());

		// Collect all entries to delete, grouped by table
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>> = HashMap::new();

		for request in requests.drain(..) {
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
						// Record stats for the drop
						stats_callback.record_drop(
							Tier::Hot,
							request.key.as_ref(),
							entry.versioned_key.len() as u64,
							entry.value_bytes,
							request.version,
						);

						// Queue for deletion (None value = delete)
						batches
							.entry(request.table)
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
	}
}

impl Drop for DropWorker {
	fn drop(&mut self) {
		self.stop();
	}
}
