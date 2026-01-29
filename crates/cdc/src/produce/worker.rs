// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Single-threaded CDC worker for background CDC generation.
//!
//! This module provides a simplified CDC generation worker that:
//! - Runs on a single background thread
//! - Uses an unbounded channel for non-blocking event delivery
//! - Processes commits sequentially and writes to CdcStore

use std::{
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
	delta::Delta,
	event::{
		EventBus,
		metric::{CdcEntryDrop, CdcEntryStats, CdcStatsDroppedEvent, CdcStatsRecordedEvent},
	},
	interface::{
		cdc::{Cdc, CdcChange, CdcSequencedChange},
		store::MultiVersionGetPrevious,
	},
	key::{Key, cdc_exclude::should_exclude_from_cdc},
};
use tracing::{debug, error, info, trace};

use crate::{
	consume::{host::CdcHost, watermark::compute_watermark},
	storage::CdcStorage,
};

/// Timeout for recv in worker loop - allows checking shutdown flag
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Default interval between CDC cleanup attempts (30 seconds)
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

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
	pub fn spawn<S, T, H>(storage: S, transaction_store: T, event_bus: EventBus, host: H) -> Self
	where
		S: CdcStorage + Send + 'static,
		T: MultiVersionGetPrevious + Clone + Send + Sync + 'static,
		H: CdcHost,
	{
		let (sender, receiver) = unbounded();
		let running = Arc::new(AtomicBool::new(true));
		let running_clone = running.clone();

		let handle = thread::Builder::new()
			.name("cdc-worker".to_string())
			.spawn(move || {
				info!("CDC worker started");
				worker_loop(storage, transaction_store, receiver, running_clone, event_bus, host);
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

fn worker_loop<S, T, H>(
	storage: S,
	transaction_store: T,
	receiver: Receiver<CdcWorkItem>,
	running: Arc<AtomicBool>,
	event_bus: EventBus,
	host: H,
) where
	S: CdcStorage,
	T: MultiVersionGetPrevious,
	H: CdcHost,
{
	let mut last_cleanup = Instant::now();

	while running.load(Ordering::SeqCst) {
		if last_cleanup.elapsed() >= CLEANUP_INTERVAL {
			if let Err(e) = try_cleanup(&storage, &host, &event_bus) {
				error!("CDC cleanup failed: {:?}", e);
			}
			last_cleanup = Instant::now();
		}

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

fn try_cleanup<S: CdcStorage, H: CdcHost>(storage: &S, host: &H, event_bus: &EventBus) -> reifydb_type::Result<()> {
	let mut txn = host.begin_command()?;
	let watermark = compute_watermark(&mut txn)?;
	txn.rollback()?;

	let result = storage.drop_before(watermark)?;
	if result.count > 0 {
		debug!(watermark = watermark.0, deleted = result.count, "CDC cleanup completed");

		let drop_entries: Vec<CdcEntryDrop> = result
			.entries
			.into_iter()
			.map(|e| CdcEntryDrop {
				key: e.key,
				value_bytes: e.value_bytes,
			})
			.collect();

		event_bus.emit(CdcStatsDroppedEvent::new(drop_entries, watermark));
	}
	Ok(())
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
			Delta::Set {
				key,
				values,
			} => {
				// Check if previous version exists to determine Insert vs Update
				let pre = transaction_store.get_previous_version(&key, item.version).ok().flatten();

				if let Some(prev_values) = pre {
					// Has previous version - this is an update
					CdcChange::Update {
						key,
						pre: prev_values.values,
						post: values,
					}
				} else {
					// No previous version - this is an insert
					CdcChange::Insert {
						key,
						post: values,
					}
				}
			}
			Delta::Unset {
				key,
				values,
			} => {
				let pre = if values.is_empty() {
					None
				} else {
					Some(values)
				};
				CdcChange::Delete {
					key,
					pre,
				}
			}
			Delta::Remove {
				..
			}
			| Delta::Drop {
				..
			} => {
				// Remove (untracked) and Drop operations never generate CDC
				continue;
			}
		};

		changes.push(CdcSequencedChange {
			sequence: seq,
			change,
		});
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

		event_bus.emit(CdcStatsRecordedEvent::new(entries, item.version));
	}
}

#[cfg(test)]
pub mod tests {
	use std::{thread::sleep, time::Duration};

	use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey};
	use reifydb_runtime::{SharedRuntimeConfig, actor::system::ActorSystem, clock::Clock};
	use reifydb_store_multi::MultiStore;
	use reifydb_store_single::SingleStore;
	use reifydb_transaction::{
		interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction,
		single::SingleTransaction, transaction::command::CommandTransaction,
	};
	use reifydb_type::util::cowvec::CowVec;

	use super::*;
	use crate::storage::memory::MemoryCdcStorage;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_values(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[derive(Clone)]
	struct TestCdcHost {
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
	}

	impl TestCdcHost {
		fn new() -> Self {
			let multi_store = MultiStore::testing_memory();
			let single_store = SingleStore::testing_memory();
			let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
			let event_bus = EventBus::new(&actor_system);
			let single = SingleTransaction::new(single_store, event_bus.clone());
			let multi = MultiTransaction::new(
				multi_store,
				single.clone(),
				event_bus.clone(),
				actor_system,
				Clock::default(),
			)
			.unwrap();
			Self {
				multi,
				single,
				event_bus,
			}
		}
	}

	impl CdcHost for TestCdcHost {
		fn begin_command(&self) -> reifydb_type::Result<CommandTransaction> {
			CommandTransaction::new(
				self.multi.clone(),
				self.single.clone(),
				self.event_bus.clone(),
				Interceptors::new(),
			)
		}

		fn current_version(&self) -> reifydb_type::Result<CommitVersion> {
			Ok(CommitVersion(1))
		}

		fn done_until(&self) -> CommitVersion {
			CommitVersion(1)
		}

		fn wait_for_mark_timeout(&self, _version: CommitVersion, _timeout: Duration) -> bool {
			true
		}
	}

	#[test]
	fn test_worker_processes_insert() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let worker = CdcWorker::spawn(storage.clone(), resolver, event_bus, host);

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
			CdcChange::Insert {
				key,
				post,
			} => {
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
		let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let worker = CdcWorker::spawn(storage.clone(), resolver, event_bus, host);

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
