// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Background worker for metrics processing.
//!
//! This module provides the single-writer MetricsWorker that processes
//! stats events off the critical commit path using an unbounded channel.
//! This ensures the hot path never blocks, even when the metrics worker
//! falls behind. Backpressure monitoring provides visibility into worker health.

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
	time::Duration,
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use reifydb_core::{
	common::CommitVersion,
	event::{
		EventBus, EventListener,
		metric::{CdcStatsRecordedEvent, StorageStatsRecordedEvent},
		store::StatsProcessedEvent,
	},
	interface::store::{MultiVersionGetPrevious, SingleVersionStore},
};
use tracing::{debug, error, trace};

use crate::{
	cdc::{CdcOperation, CdcStatsWriter},
	multi::{MultiStorageOperation, StorageStatsWriter, Tier},
};

/// Configuration for the metrics worker.
#[derive(Debug, Clone)]
pub struct MetricsWorkerConfig {
	/// Threshold for warning about channel backlog (default: 50,000).
	/// When the number of pending events exceeds this threshold, warnings will be logged.
	pub backpressure_warning_threshold: usize,
}

impl Default for MetricsWorkerConfig {
	fn default() -> Self {
		Self {
			backpressure_warning_threshold: 50_000,
		}
	}
}

/// Event types for metrics tracking - separated by subsystem.
#[derive(Debug, Clone)]
pub enum MetricsEvent {
	/// Multi-version storage operations batch.
	Multi {
		ops: Vec<MultiStorageOperation>,
		version: CommitVersion,
	},
	/// CDC operations batch.
	Cdc {
		ops: Vec<CdcOperation>,
		version: CommitVersion,
	},
	/// Shutdown the worker.
	Shutdown,
}

/// Background worker that owns the writers.
///
/// This is the ONLY writer - ensures single-writer semantics.
pub struct MetricsWorker {
	sender: Sender<MetricsEvent>,
	running: Arc<AtomicBool>,
	worker: Option<JoinHandle<()>>,
}

impl MetricsWorker {
	/// Create and start a new metrics worker.
	///
	/// # Arguments
	/// - `config`: Worker configuration
	/// - `storage`: Single-version storage for metrics persistence
	/// - `resolver`: Multi-version store for looking up previous versions
	/// - `event_bus`: Event bus for emitting stats processed events
	pub fn new<S, R>(config: MetricsWorkerConfig, storage: S, resolver: R, event_bus: EventBus) -> Self
	where
		S: SingleVersionStore,
		R: MultiVersionGetPrevious + Clone + Send + Sync + 'static,
	{
		let (sender, receiver) = unbounded();
		let running = Arc::new(AtomicBool::new(true));
		let backpressure_threshold = config.backpressure_warning_threshold;

		let worker_running = Arc::clone(&running);
		let worker = thread::Builder::new()
			.name("metrics-worker".to_string())
			.spawn(move || {
				Self::worker_loop(
					receiver,
					storage,
					resolver,
					worker_running,
					event_bus,
					backpressure_threshold,
				);
			})
			.expect("Failed to spawn metrics worker thread");

		Self {
			sender,
			running,
			worker: Some(worker),
		}
	}

	/// Stop the worker gracefully.
	pub fn stop(&mut self) {
		if !self.running.swap(false, Ordering::AcqRel) {
			return;
		}

		// Send shutdown signal
		let _ = self.sender.send(MetricsEvent::Shutdown);

		// Wait for worker to finish
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}

	/// Check if the worker is running.
	pub fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	/// Get a clone of the sender for use by event listeners.
	pub fn sender(&self) -> Sender<MetricsEvent> {
		self.sender.clone()
	}

	fn worker_loop<S, R>(
		receiver: Receiver<MetricsEvent>,
		storage: S,
		resolver: R,
		running: Arc<AtomicBool>,
		event_bus: EventBus,
		backpressure_threshold: usize,
	) where
		S: SingleVersionStore,
		R: MultiVersionGetPrevious,
	{
		debug!("Metrics worker started");

		let mut storage_writer = StorageStatsWriter::new(storage.clone());
		let mut cdc_writer = CdcStatsWriter::new(storage);
		let mut max_version = CommitVersion(0);

		while running.load(Ordering::Acquire) {
			// Check backlog before processing
			let backlog = receiver.len();
			if backlog > backpressure_threshold {
				error!(
					"Metrics worker backlog HIGH: {} events (threshold: {}). Worker may be falling behind.",
					backlog, backpressure_threshold
				);
			} else if backlog > backpressure_threshold / 2 {
				debug!("Metrics worker backlog elevated: {} events", backlog);
			}

			match receiver.recv_timeout(Duration::from_millis(100)) {
				Ok(event) => {
					match event {
						MetricsEvent::Multi {
							ops,
							version,
						} => {
							trace!(
								"Processing {} multi-storage ops for version {:?}",
								ops.len(),
								version
							);

							// Collect dropped keys first - if a key is dropped in this
							// batch, any write to that key is a fresh insert (not an
							// update to the old entry)
							let dropped_keys: std::collections::HashSet<_> = ops
								.iter()
								.filter_map(|op| match op {
									MultiStorageOperation::Drop {
										key,
										..
									} => Some(key.clone()),
									_ => None,
								})
								.collect();

							for op in ops {
								match op {
									MultiStorageOperation::Write {
										tier,
										key,
										value_bytes,
									} => {
										// If the key was dropped in this same
										// batch (e.g., ringbuffer eviction),
										// this is a fresh insert, not an update
										let pre_value_bytes = if dropped_keys
											.contains(&key)
										{
											None
										} else {
											resolver.get_previous_version(
												&key, version,
											)
											.ok()
											.flatten()
											.map(|v| v.values.len() as u64)
										};

										if let Err(e) = storage_writer
											.record_write(
												tier,
												key.as_ref(),
												value_bytes,
												pre_value_bytes,
											) {
											error!(
												"Failed to record write: {}",
												e
											);
										}
									}
									MultiStorageOperation::Delete {
										tier,
										key,
										value_bytes,
									} => {
										// value_bytes comes from the event - no
										// lookup needed
										if let Err(e) = storage_writer
											.record_delete(
												tier,
												key.as_ref(),
												Some(value_bytes),
											) {
											error!(
												"Failed to record delete: {}",
												e
											);
										}
									}
									MultiStorageOperation::Drop {
										tier,
										key,
										value_bytes,
									} => {
										if let Err(e) = storage_writer
											.record_drop(
												tier,
												key.as_ref(),
												value_bytes,
											) {
											error!(
												"Failed to record drop: {}",
												e
											);
										}
									}
								}
							}
							if version > max_version {
								max_version = version;
							}
						}
						MetricsEvent::Cdc {
							ops,
							version,
						} => {
							trace!(
								"Processing {} CDC ops for version {:?}",
								ops.len(),
								version
							);
							for op in ops {
								if let Err(e) = cdc_writer
									.record_cdc(op.key.as_ref(), op.value_bytes)
								{
									error!("Failed to record cdc: {}", e);
								}
							}
							if version > max_version {
								max_version = version;
							}
						}
						MetricsEvent::Shutdown => {
							debug!("Metrics worker received shutdown signal");
							Self::emit_stats_processed(&event_bus, &mut max_version);
							break;
						}
					}
				}
				Err(RecvTimeoutError::Timeout) => {
					Self::emit_stats_processed(&event_bus, &mut max_version);
				}
				Err(RecvTimeoutError::Disconnected) => {
					debug!("Metrics worker channel disconnected");
					Self::emit_stats_processed(&event_bus, &mut max_version);
					break;
				}
			}
		}

		debug!("Metrics worker stopped");
	}

	fn emit_stats_processed(event_bus: &EventBus, max_version: &mut CommitVersion) {
		if max_version.0 > 0 {
			event_bus.emit(StatsProcessedEvent::new(*max_version));
			*max_version = CommitVersion(0);
		}
	}
}

impl Drop for MetricsWorker {
	fn drop(&mut self) {
		self.stop();
	}
}

/// Event listener for storage stats events.
///
/// Converts `StorageStatsRecordedEvent` into `MetricsEvent::Multi` and sends
/// to the metrics worker for processing.
pub struct StorageStatsListener {
	sender: Sender<MetricsEvent>,
}

impl StorageStatsListener {
	/// Create a new storage stats listener.
	pub fn new(sender: Sender<MetricsEvent>) -> Self {
		Self {
			sender,
		}
	}
}

impl EventListener<StorageStatsRecordedEvent> for StorageStatsListener {
	fn on(&self, event: &StorageStatsRecordedEvent) {
		let mut ops = Vec::with_capacity(event.writes().len() + event.deletes().len() + event.drops().len());

		for write in event.writes() {
			ops.push(MultiStorageOperation::Write {
				tier: Tier::Hot,
				key: write.key.clone(),
				value_bytes: write.value_bytes,
			});
		}

		for delete in event.deletes() {
			ops.push(MultiStorageOperation::Delete {
				tier: Tier::Hot,
				key: delete.key.clone(),
				value_bytes: delete.value_bytes,
			});
		}

		for drop in event.drops() {
			ops.push(MultiStorageOperation::Drop {
				tier: Tier::Hot,
				key: drop.key.clone(),
				value_bytes: drop.value_bytes,
			});
		}

		let _ = self.sender.send(MetricsEvent::Multi {
			ops,
			version: *event.version(),
		});
	}
}

/// Event listener for CDC stats events.
pub struct CdcStatsListener {
	sender: Sender<MetricsEvent>,
}

impl CdcStatsListener {
	pub fn new(sender: Sender<MetricsEvent>) -> Self {
		Self {
			sender,
		}
	}
}

impl EventListener<CdcStatsRecordedEvent> for CdcStatsListener {
	fn on(&self, event: &CdcStatsRecordedEvent) {
		let ops: Vec<CdcOperation> = event
			.entries()
			.iter()
			.map(|entry| CdcOperation {
				key: entry.key.clone(),
				value_bytes: entry.value_bytes,
			})
			.collect();

		if !ops.is_empty() {
			let _ = self.sender.send(MetricsEvent::Cdc {
				ops,
				version: *event.version(),
			});
		}
	}
}
