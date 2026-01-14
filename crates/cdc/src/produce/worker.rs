// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Single-threaded CDC worker for background CDC generation.
//!
//! This module provides a simplified CDC generation worker that:
//! - Runs on a single background thread
//! - Uses an unbounded channel for non-blocking event delivery
//! - Processes commits sequentially and writes to CdcStore

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, unbounded};
use reifydb_core::{
	CommitVersion,
	delta::Delta,
	event::{CdcEntryStats, CdcStatsRecordedEvent, EventBus},
	interface::{Cdc, CdcChange, CdcSequencedChange, MultiVersionGetPrevious},
	key::{Key, should_exclude_from_cdc},
};
use tracing::{error, info, trace};
use crate::storage::CdcStorage;

/// Timeout for recv in worker loop - allows checking shutdown flag
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Work item sent to the CDC worker.
pub struct CdcWorkItem {
	/// The commit version.
	pub version: CommitVersion,
	/// Timestamp in milliseconds since epoch.
	pub timestamp: u64,
	/// The deltas from the commit (already optimized).
	pub deltas: Vec<Delta>,
}

/// Single-threaded CDC worker with unbounded backlog.
///
/// Spawns a background thread that processes commits and generates CDC entries.
/// Uses an unbounded channel to ensure non-blocking delivery from the event handler.
pub struct CdcWorker {
	sender: Sender<CdcWorkItem>,
	handle: Option<JoinHandle<()>>,
	running: Arc<AtomicBool>,
}

impl CdcWorker {
	/// Spawn a new CDC worker.
	///
	/// # Arguments
	/// - `storage`: The CDC storage backend to write entries to
	/// - `transaction_store`: Transaction store for looking up previous versions (needed for Update/Delete CDC)
	/// - `event_bus`: Event bus for emitting CDC stats events
	pub fn spawn<S, T>(storage: S, transaction_store: T, event_bus: EventBus) -> Self
	where
		S: CdcStorage + Send + 'static,
		T: MultiVersionGetPrevious + Clone + Send + Sync + 'static,
	{
		let (sender, receiver) = unbounded();
		let running = Arc::new(AtomicBool::new(true));
		let running_clone = running.clone();

		let handle = thread::Builder::new()
			.name("cdc-worker".to_string())
			.spawn(move || {
				info!("CDC worker started");
				worker_loop(storage, transaction_store, receiver, running_clone, event_bus);
				info!("CDC worker stopped");
			})
			.expect("Failed to spawn CDC worker");

		Self {
			sender,
			handle: Some(handle),
			running,
		}
	}

	/// Non-blocking send to worker backlog.
	///
	/// This will never block. If the worker is slow, items queue up in memory.
	pub fn send(&self, item: CdcWorkItem) {
		let _ = self.sender.try_send(item);
	}

	/// Get a clone of the sender for use by event listeners.
	pub fn sender(&self) -> Sender<CdcWorkItem> {
		self.sender.clone()
	}

	/// Shutdown the worker gracefully.
	///
	/// Sets the running flag to false, which causes the worker to exit
	/// on the next recv timeout. Then we join the thread.
	pub fn shutdown(&mut self) {
		self.running.store(false, Ordering::SeqCst);
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

impl Drop for CdcWorker {
	fn drop(&mut self) {
		// Set running to false so worker exits on next timeout
		self.running.store(false, Ordering::SeqCst);
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

fn worker_loop<S, T>(
	storage: S,
	transaction_store: T,
	receiver: Receiver<CdcWorkItem>,
	running: Arc<AtomicBool>,
	event_bus: EventBus,
)
where
	S: CdcStorage,
	T: MultiVersionGetPrevious,
{
	while running.load(Ordering::SeqCst) {
		match receiver.recv_timeout(RECV_TIMEOUT) {
			Ok(item) => {
				process_work_item(&storage, &transaction_store, item, &event_bus);
			}
			Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
				// Just check the running flag and continue
			}
			Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
				// Channel closed, exit
				break;
			}
		}
	}
}

fn process_work_item<S, T>(storage: &S, transaction_store: &T, item: CdcWorkItem, event_bus: &EventBus)
where
	S: CdcStorage,
	T: MultiVersionGetPrevious,
{
	let mut changes = Vec::new();
	let mut seq = 0u16;

	trace!(version = item.version.0, delta_count = item.deltas.len(), "Processing CDC work item");

	for delta in item.deltas {
		let key = delta.key().clone();

		// Skip internal system keys that shouldn't appear in CDC
		if let Some(kind) = Key::kind(&key) {
			if should_exclude_from_cdc(kind) {
				continue;
			}
		}

		seq += 1;

		let change = match delta {
			Delta::Set { key, values } => {
				// Check if previous version exists to determine Insert vs Update
				let pre = transaction_store
					.get_previous_version(&key, item.version)
					.ok()
					.flatten();

				if let Some(prev_values) = pre {
					// Has previous version - this is an update
					CdcChange::Update {
						key,
						pre: prev_values.values,
						post: values,
					}
				} else {
					// No previous version - this is an insert
					CdcChange::Insert { key, post: values }
				}
			}
			Delta::Unset { key, values } => {
				let pre = if values.is_empty() { None } else { Some(values) };
				CdcChange::Delete { key, pre }
			}
			Delta::Remove { .. } | Delta::Drop { .. } => {
				// Remove (untracked) and Drop operations never generate CDC
				continue;
			}
		};

		changes.push(CdcSequencedChange { sequence: seq, change });
	}

	if !changes.is_empty() {
		let cdc = Cdc::new(item.version, item.timestamp, changes.clone());
		if let Err(e) = storage.write(&cdc) {
			error!(version = item.version.0, "CDC write failed: {:?}", e);
			return;
		}

		// Emit CDC stats event
		let entries: Vec<CdcEntryStats> = changes
			.iter()
			.map(|seq_change| {
				let key = seq_change.change.key();
				let value_bytes = seq_change.change.value_bytes() as u64;
				CdcEntryStats {
					key: key.clone(),
					value_bytes,
				}
			})
			.collect();

		event_bus.emit(CdcStatsRecordedEvent {
			entries,
			version: item.version,
		});
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::MemoryCdcStorage;
	use reifydb_core::{CowVec, EncodedKey, value::encoded::EncodedValues};
	use reifydb_store_multi::MultiStore;
	use std::thread::sleep;
	use std::time::Duration;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_values(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_worker_processes_insert() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let event_bus = EventBus::new();
		let worker = CdcWorker::spawn(storage.clone(), resolver, event_bus);

		let deltas = vec![Delta::Set {
			key: make_key("test_key"),
			values: make_values("test_value"),
		}];

		worker.send(CdcWorkItem {
			version: CommitVersion(1),
			timestamp: 12345,
			deltas,
		});

		// Give worker time to process
		sleep(Duration::from_millis(50));

		let cdc = storage.read(CommitVersion(1)).unwrap();
		assert!(cdc.is_some());
		let cdc = cdc.unwrap();
		assert_eq!(cdc.version, CommitVersion(1));
		assert_eq!(cdc.changes.len(), 1);

		match &cdc.changes[0].change {
			CdcChange::Insert { key, post } => {
				assert_eq!(key.as_ref(), b"test_key");
				assert_eq!(post.0.as_slice(), b"test_value");
			}
			_ => panic!("Expected Insert change"),
		}
	}

	#[test]
	fn test_worker_skips_drop_operations() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let event_bus = EventBus::new();
		let worker = CdcWorker::spawn(storage.clone(), resolver, event_bus);

		let deltas = vec![
			Delta::Set {
				key: make_key("key1"),
				values: make_values("value1"),
			},
			Delta::Drop {
				key: make_key("key2"),
				up_to_version: None,
				keep_last_versions: None,
			},
		];

		worker.send(CdcWorkItem {
			version: CommitVersion(2),
			timestamp: 12345,
			deltas,
		});

		sleep(Duration::from_millis(50));

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// Only the Set should produce CDC, not the Drop
		assert_eq!(cdc.changes.len(), 1);
	}
}
