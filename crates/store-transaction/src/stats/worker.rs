// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Background worker for storage statistics tracking.
//!
//! This module provides an asynchronous stats tracking worker that processes
//! stats events off the critical commit path. Stats are collected via channels
//! and processed in a dedicated background thread.

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
	time::Duration,
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use reifydb_core::{CommitVersion, event::EventBus, event::store::StatsProcessed};
use tracing::{debug, error, trace};

use super::{PreVersionInfo, StorageTracker, Tier};
use crate::hot::HotStorage;

/// Configuration for the stats worker.
#[derive(Debug, Clone)]
pub struct StatsWorkerConfig {
	/// Maximum number of pending events in the channel.
	/// If the channel is full, events will be dropped (fire-and-forget).
	pub channel_capacity: usize,
	/// How often to flush accumulated stats to storage.
	pub checkpoint_interval: Duration,
}

impl Default for StatsWorkerConfig {
	fn default() -> Self {
		Self {
			channel_capacity: 10_000,
			checkpoint_interval: Duration::from_secs(10),
		}
	}
}

/// A single stats operation to be processed.
#[derive(Debug, Clone)]
pub enum StatsOp {
	/// Record a write operation.
	Write {
		tier: Tier,
		key: Vec<u8>,
		key_bytes: u64,
		value_bytes: u64,
		pre_info: Option<PreVersionInfo>,
	},
	/// Record a delete operation.
	Delete {
		tier: Tier,
		key: Vec<u8>,
		key_bytes: u64,
		pre_info: Option<PreVersionInfo>,
	},
	/// Record a drop operation (physical removal).
	Drop {
		tier: Tier,
		key: Vec<u8>,
		versioned_key_bytes: u64,
		value_bytes: u64,
	},
	/// Record CDC bytes for a change.
	Cdc {
		tier: Tier,
		key: Vec<u8>,
		value_bytes: u64,
		count: u64,
	},
}

/// Event types for stats tracking.
#[derive(Debug, Clone)]
pub enum StatsEvent {
	/// Batch of stats operations from a single commit.
	Batch {
		ops: Vec<StatsOp>,
		version: CommitVersion,
	},
	/// Force a checkpoint to storage.
	Checkpoint,
	/// Shutdown the worker.
	Shutdown,
}

/// Background worker for processing stats events.
pub struct StatsWorker {
	sender: Sender<StatsEvent>,
	running: Arc<AtomicBool>,
	worker: Option<JoinHandle<()>>,
}

impl StatsWorker {
	/// Create and start a new stats worker.
	pub fn new(
		config: StatsWorkerConfig,
		tracker: StorageTracker,
		storage: HotStorage,
		event_bus: EventBus,
	) -> Self {
		let (sender, receiver) = bounded(config.channel_capacity);
		let running = Arc::new(AtomicBool::new(true));

		let worker_running = Arc::clone(&running);
		let worker = thread::Builder::new()
			.name("store-stats".to_string())
			.spawn(move || {
				Self::worker_loop(
					receiver,
					tracker,
					storage,
					config.checkpoint_interval,
					worker_running,
					event_bus,
				);
			})
			.expect("Failed to spawn store stats thread");

		Self {
			sender,
			running,
			worker: Some(worker),
		}
	}

	/// Queue a batch of stats operations for processing.
	/// This is the preferred method for commits that produce multiple stats events.
	#[inline]
	pub fn record_batch(&self, ops: Vec<StatsOp>, version: CommitVersion) {
		if !ops.is_empty() {
			let _ = self.sender.try_send(StatsEvent::Batch { ops, version });
		}
	}

	/// Queue a drop event for processing.
	/// Used by the drop worker which already batches its own operations.
	#[inline]
	pub fn record_drop(
		&self,
		tier: Tier,
		key: &[u8],
		versioned_key_bytes: u64,
		value_bytes: u64,
		version: CommitVersion,
	) {
		let op = StatsOp::Drop {
			tier,
			key: key.to_vec(),
			versioned_key_bytes,
			value_bytes,
		};
		let _ = self.sender.try_send(StatsEvent::Batch { ops: vec![op], version });
	}

	/// Request a checkpoint.
	pub fn checkpoint(&self) {
		let _ = self.sender.try_send(StatsEvent::Checkpoint);
	}

	/// Stop the worker gracefully.
	pub fn stop(&mut self) {
		if !self.running.swap(false, Ordering::AcqRel) {
			return;
		}

		// Send shutdown signal
		let _ = self.sender.send(StatsEvent::Shutdown);

		// Wait for worker to finish
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}

	/// Check if the worker is running.
	pub fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	fn worker_loop(
		receiver: Receiver<StatsEvent>,
		tracker: StorageTracker,
		storage: HotStorage,
		checkpoint_interval: Duration,
		running: Arc<AtomicBool>,
		event_bus: EventBus,
	) {
		debug!("Stats worker started");

		let mut last_checkpoint = std::time::Instant::now();
		let mut events_since_checkpoint = 0u64;
		let mut max_version = CommitVersion(0);

		while running.load(Ordering::Acquire) {
			// Use timeout to allow periodic checkpoint checks
			match receiver.recv_timeout(Duration::from_millis(100)) {
				Ok(event) => {
					match event {
						StatsEvent::Batch { ops, version } => {
							for op in ops {
								match op {
									StatsOp::Write {
										tier,
										key,
										key_bytes,
										value_bytes,
										pre_info,
									} => {
										tracker.record_write(tier, &key, key_bytes, value_bytes, pre_info);
									}
									StatsOp::Delete {
										tier,
										key,
										key_bytes,
										pre_info,
									} => {
										tracker.record_delete(tier, &key, key_bytes, pre_info);
									}
									StatsOp::Drop {
										tier,
										key,
										versioned_key_bytes,
										value_bytes,
									} => {
										tracker.record_drop(tier, &key, versioned_key_bytes, value_bytes);
									}
									StatsOp::Cdc {
										tier,
										key,
										value_bytes,
										count,
									} => {
										tracker.record_cdc_for_change(tier, &key, value_bytes, count);
									}
								}
								events_since_checkpoint += 1;
							}
							if version > max_version {
								max_version = version;
							}
						}
						StatsEvent::Checkpoint => {
							Self::do_checkpoint(
								&tracker,
								&storage,
								&mut last_checkpoint,
								&mut events_since_checkpoint,
							);
							Self::emit_stats_processed(&event_bus, &mut max_version);
						}
						StatsEvent::Shutdown => {
							debug!("Stats worker received shutdown signal");
							// Final checkpoint before shutdown
							Self::do_checkpoint(
								&tracker,
								&storage,
								&mut last_checkpoint,
								&mut events_since_checkpoint,
							);
							Self::emit_stats_processed(&event_bus, &mut max_version);
							break;
						}
					}
				}
				Err(RecvTimeoutError::Timeout) => {
					// Emit stats processed on timeout if we have pending versions
					Self::emit_stats_processed(&event_bus, &mut max_version);
				}
				Err(RecvTimeoutError::Disconnected) => {
					debug!("Stats worker channel disconnected");
					Self::emit_stats_processed(&event_bus, &mut max_version);
					break;
				}
			}

			// Periodic checkpoint
			if last_checkpoint.elapsed() >= checkpoint_interval && events_since_checkpoint > 0 {
				Self::do_checkpoint(
					&tracker,
					&storage,
					&mut last_checkpoint,
					&mut events_since_checkpoint,
				);
				Self::emit_stats_processed(&event_bus, &mut max_version);
			}
		}

		debug!("Stats worker stopped");
	}

	fn emit_stats_processed(event_bus: &EventBus, max_version: &mut CommitVersion) {
		if max_version.0 > 0 {
			event_bus.emit(StatsProcessed { up_to: *max_version });
			*max_version = CommitVersion(0);
		}
	}

	fn do_checkpoint(
		tracker: &StorageTracker,
		storage: &HotStorage,
		last_checkpoint: &mut std::time::Instant,
		events_since_checkpoint: &mut u64,
	) {
		trace!("Stats worker checkpointing {} events", events_since_checkpoint);
		if let Err(e) = tracker.checkpoint(storage) {
			error!("Stats worker checkpoint failed: {}", e);
		}
		*last_checkpoint = std::time::Instant::now();
		*events_since_checkpoint = 0;
	}
}

impl Drop for StatsWorker {
	fn drop(&mut self) {
		self.stop();
	}
}
