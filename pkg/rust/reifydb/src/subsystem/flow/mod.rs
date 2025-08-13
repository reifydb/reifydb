// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	any::Any,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread::{self, JoinHandle},
	time::Duration,
};

use reifydb_core::{
	Result, Version,
	interface::{
		CdcEvent, CdcTransaction, Change, Engine as _, Transaction,
	},
};
use reifydb_engine::Engine;

use super::Subsystem;
use crate::health::HealthStatus;

pub struct FlowSubsystem<T: Transaction> {
	engine: Engine<T>,
	poll_interval: Duration,
	running: Arc<AtomicBool>,
	last_seen_version: Arc<AtomicU64>,
	handle: Option<JoinHandle<()>>,
}

impl<T: Transaction> FlowSubsystem<T> {
	pub fn new(engine: Engine<T>, poll_interval: Duration) -> Self {
		Self {
			engine,
			poll_interval,
			running: Arc::new(AtomicBool::new(false)),
			last_seen_version: Arc::new(AtomicU64::new(1)),
			handle: None,
		}
	}

	pub fn last_seen_version(&self) -> Version {
		self.last_seen_version.load(Ordering::Relaxed)
	}
}

impl<T: Transaction> FlowSubsystem<T> {
	fn poll_and_print_events(
		engine: &Engine<T>,
		last_seen_version: &AtomicU64,
	) -> Result<()> {
		let query_txn = engine.begin_query()?;

		let current_last_seen =
			last_seen_version.load(Ordering::Relaxed);

		let events: Vec<CdcEvent> = query_txn
			.cdc()
			.range(
				Bound::Excluded(current_last_seen),
				Bound::Included(current_last_seen + 1),
			)?
			.collect();

		if !events.is_empty() {
			last_seen_version.store(
				current_last_seen + 1,
				Ordering::Relaxed,
			);
		}

		// let mut new_events_found = false;
		let mut max_version_seen = current_last_seen;

		for event in events {
			// if event.version > current_last_seen {
			Self::print_cdc_event(&event);
			max_version_seen = max_version_seen.max(event.version);
			// new_events_found = true;
			// }
		}

		dbg!(last_seen_version.load(Ordering::Relaxed));

		// if new_events_found {

		// }

		Ok(())
	}

	fn print_cdc_event(event: &CdcEvent) {
		let change_description =
			match &event.change {
				Change::Insert {
					key,
					after,
				} => {
					format!(
						"INSERT key={:?} value={:?}",
						String::from_utf8_lossy(&key.0),
						String::from_utf8_lossy(
							&after.0
						)
					)
				}
				Change::Update {
					key,
					before,
					after,
				} => {
					let before_str =
						if before.is_deleted() {
							"<deleted>".to_string()
						} else {
							format!("{:?}", String::from_utf8_lossy(&before.0))
						};
					format!(
						"UPDATE key={:?} before={} after={:?}",
						String::from_utf8_lossy(&key.0),
						before_str,
						String::from_utf8_lossy(
							&after.0
						)
					)
				}
				Change::Delete {
					key,
					before,
				} => {
					let before_str =
						if before.is_deleted() {
							"<deleted>".to_string()
						} else {
							format!("{:?}", String::from_utf8_lossy(&before.0))
						};
					format!(
						"DELETE key={:?} before={}",
						String::from_utf8_lossy(&key.0),
						before_str
					)
				}
			};

		println!(
			"[CDC] v{} seq{} ts{} | {}",
			event.version,
			event.sequence,
			event.timestamp,
			change_description
		);
	}
}

impl<T: Transaction> Drop for FlowSubsystem<T> {
	fn drop(&mut self) {
		let _ = self.stop();
	}
}

impl<T: Transaction + Send + Sync> Subsystem for FlowSubsystem<T> {
	fn name(&self) -> &'static str {
		"Flow"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already running
		}

		self.running.store(true, Ordering::Relaxed);

		let engine = self.engine.clone();
		let poll_interval = self.poll_interval;
		let running = Arc::clone(&self.running);
		let last_seen_version = Arc::clone(&self.last_seen_version);

		let handle = thread::spawn(move || {
			println!(
				"[FlowSubsystem] Started CDC event polling with interval {:?}",
				poll_interval
			);

			while running.load(Ordering::Relaxed) {
				if let Err(e) = Self::poll_and_print_events(
					&engine,
					&last_seen_version,
				) {
					eprintln!(
						"[FlowSubsystem] Error polling CDC events: {}",
						e
					);
				}

				thread::sleep(poll_interval);
			}

			println!("[FlowSubsystem] Stopped CDC event polling");
		});

		self.handle = Some(handle);
		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already stopped
		}

		self.running.store(false, Ordering::Relaxed);

		if let Some(handle) = self.handle.take() {
			handle.join()
				.expect("Failed to join flow subsystem thread");
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Relaxed)
	}

	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
