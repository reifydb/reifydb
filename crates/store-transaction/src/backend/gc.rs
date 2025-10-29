// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::mpsc::{self, Receiver, Sender},
	thread::{self, JoinHandle},
	time::Duration,
};

use reifydb_type::Result;

/// Statistics collected during garbage collection
#[derive(Debug, Clone, Default)]
pub struct GcStats {
	/// Number of keys processed during GC
	pub keys_processed: usize,
	/// Number of old versions removed
	pub versions_removed: usize,
	/// Number of tables clean
	pub tables_cleaned: usize,
}

impl GcStats {
	pub fn merge(&mut self, other: GcStats) {
		self.keys_processed += other.keys_processed;
		self.versions_removed += other.versions_removed;
		self.tables_cleaned += other.tables_cleaned;
	}
}

/// Trait for backends that support garbage collection of old versions
pub trait BackendGarbageCollect: Send + Sync {
	/// Compact operator states by removing old versions, keeping only the latest
	fn compact_operator_states(&self) -> Result<GcStats>;
}

enum GcCommand {
	Shutdown,
}

/// Manages a background thread that periodically runs garbage collection
pub struct GarbageCollector {
	thread_handle: Option<JoinHandle<()>>,
	shutdown_sender: Sender<GcCommand>,
}

impl GarbageCollector {
	/// Spawn a new garbage collector thread
	///
	/// The GC thread will periodically call `compact_operator_states()` on the backend
	/// at the specified interval.
	pub fn spawn<B: BackendGarbageCollect + Clone + 'static>(
		backend: B,
		interval: Duration,
	) -> Self {
		let (shutdown_sender, shutdown_receiver) = mpsc::channel();

		let thread_handle = thread::spawn(move || {
			Self::run(backend, interval, shutdown_receiver);
		});

		Self {
			thread_handle: Some(thread_handle),
			shutdown_sender,
		}
	}

	/// Main GC loop - runs with smart backoff strategy
	fn run<B: BackendGarbageCollect>(
		backend: B,
		interval: Duration,
		shutdown_receiver: Receiver<GcCommand>,
	) {
		let mut current_delay = Duration::from_secs(0); // Start immediately
		let max_delay = interval; // Maximum delay (typically 10 seconds)

		loop {
			// Wait for current_delay or shutdown signal
			match shutdown_receiver.recv_timeout(current_delay) {
				Ok(GcCommand::Shutdown) => {
					break;
				}
				Err(mpsc::RecvTimeoutError::Timeout) => {
					// Time to run GC
					match backend.compact_operator_states() {
						Ok(stats) => {
							println!(
								"[GC] Stats: keys_processed={}, versions_removed={}, tables_cleaned={}",
								stats.keys_processed, stats.versions_removed, stats.tables_cleaned
							);

							// Continuous exponential backoff based on work done
							current_delay = if stats.versions_removed >= 1024 {
								println!("[GC] Hit batch limit ({}), running again immediately",
										 stats.versions_removed);
								Duration::from_secs(0)
							} else if stats.versions_removed == 0 {
								println!("[GC] No work found, backing off to {}s", max_delay.as_secs());
								max_delay
							} else {
								// Exponential backoff: more work → shorter delay
								// Formula: delay = max_delay × (1 - (versions_removed / 1024))
								let ratio = stats.versions_removed as f64 / 1024.0;
								let delay_secs = (max_delay.as_secs() as f64 * (1.0 - ratio)).ceil() as u64;
								let delay_secs = delay_secs.max(1); // Minimum 1 second
								let delay = Duration::from_secs(delay_secs);

								println!("[GC] Processed {} versions ({:.1}% of batch), backing off to {}s",
										 stats.versions_removed, ratio * 100.0, delay.as_secs());
								delay
							};
						}
						Err(e) => {
							eprintln!("[GC] Error during compaction: {:?}", e);
							current_delay = max_delay;
						}
					}
				}
				Err(mpsc::RecvTimeoutError::Disconnected) => {
					break;
				}
			}
		}
	}

	/// Shutdown the GC thread gracefully
	pub fn shutdown(mut self) -> Result<()> {
		if let Some(handle) = self.thread_handle.take() {
			// Send shutdown signal
			let _ = self.shutdown_sender.send(GcCommand::Shutdown);

			// Wait for thread to finish
			let _ = handle.join();
		}
		Ok(())
	}
}

impl Drop for GarbageCollector {
	fn drop(&mut self) {
		if let Some(handle) = self.thread_handle.take() {
			// Send shutdown signal
			let _ = self.shutdown_sender.send(GcCommand::Shutdown);

			// Wait for thread to finish
			let _ = handle.join();
		}
	}
}