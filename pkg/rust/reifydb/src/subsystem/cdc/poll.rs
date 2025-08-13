// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	ops::Bound,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle, sleep},
	time::Duration,
};

use reifydb_core::{
	Result, Version,
	interface::{
		ActiveCommandTransaction, CdcConsume, CdcConsumer, CdcEvent,
		CdcTransaction, ConsumerId, Engine as EngineInterface, Key,
		Transaction, VersionedCommandTransaction,
		VersionedQueryTransaction,
		key::{CdcConsumerKey, EncodableKey},
	},
	row::EncodedRow,
	util::CowVec,
};
use reifydb_engine::Engine;

/// Poll-based CDC consumer implementation
pub struct PollConsumer<T: Transaction, F: CdcConsume<T>> {
	id: ConsumerId,
	engine: Option<Engine<T>>,
	poll_interval: Duration,
	running: Option<AtomicBool>,
	handle: Arc<Mutex<Option<JoinHandle<()>>>>,
	consume: Option<Box<F>>,
}

impl<T: Transaction, F: CdcConsume<T>> PollConsumer<T, F> {
	/// Creates a new poll-based consumer with a processing function
	pub fn new(
		id: ConsumerId,
		engine: Engine<T>,
		poll_interval: Duration,
		consume: F,
	) -> Self {
		Self {
			id,
			engine: Some(engine),
			poll_interval,
			running: Some(AtomicBool::new(false)),
			handle: Arc::new(Mutex::new(None)),
			consume: Some(Box::new(consume)),
		}
	}

	/// Internal consume method that processes events
	fn consume_events(
		id: ConsumerId,
		engine: &Engine<T>,
		process_fn: &F,
	) -> Result<()> {
		let mut txn = engine.begin_command()?;

		let last_version = Self::read_last_version_for(id, &mut txn)?;

		let events: Vec<CdcEvent> = txn
			.cdc()
			.range(
				Bound::Excluded(last_version),
				Bound::Included(last_version + 1),
			)?
			.collect();

		if events.is_empty() {
			txn.rollback()?;
			return Ok(());
		}

		// if only a consumer update we ignore that
		if events.len() == 1 {
			match Key::decode(events.first().unwrap().key())
				.unwrap()
			{
				Key::CdcConsumer(_) => {
					txn.rollback()?;
					return Ok(());
				}
				_ => {}
			}
		}

		let events = events
			.into_iter()
			.filter(|e| match Key::decode(e.key()).unwrap() {
				Key::TableRow(_) => true,
				_ => false,
			})
			.collect::<Vec<_>>();

		if events.is_empty() {
			// no interesting events
			Self::update_last_version_for(
				id,
				&mut txn,
				last_version + 1,
			)?;
			txn.commit()?;
			return Ok(());
		}

		process_fn.consume(&mut txn, events)?;

		Self::update_last_version_for(id, &mut txn, last_version + 1)?;
		txn.commit()?;
		Ok(())
	}

	/// Reads the last consumed version from storage for a given consumer
	fn read_last_version_for(
		id: ConsumerId,
		txn: &mut ActiveCommandTransaction<T>,
	) -> Result<Version> {
		let key = CdcConsumerKey {
			consumer: id,
		};

		let encoded_key = key.encode();

		// Try to get the stored version
		let stored = txn.get(&encoded_key)?;

		match stored {
			Some(versioned) => {
				// Decode the stored version (assuming it's
				// stored as u64 bytes)
				if versioned.row.len() >= 8 {
					let mut bytes = [0u8; 8];
					bytes.copy_from_slice(
						&versioned.row[0..8],
					);
					Ok(u64::from_be_bytes(bytes))
				} else {
					Ok(1)
				}
			}
			None => Ok(1),
		}
	}

	/// Updates the last consumed version in storage for a given consumer
	fn update_last_version_for(
		id: ConsumerId,
		txn: &mut ActiveCommandTransaction<T>,
		version: Version,
	) -> Result<()> {
		let key = CdcConsumerKey {
			consumer: id,
		};

		let encoded_key = key.encode();
		let encoded_version = version.to_be_bytes().to_vec();

		txn.set(&encoded_key, EncodedRow(CowVec::new(encoded_version)))
	}
}

impl<T: Transaction + 'static, F: CdcConsume<T>> CdcConsumer
	for PollConsumer<T, F>
{
	fn id(&self) -> ConsumerId {
		self.id
	}

	fn start(&mut self) -> Result<()> {
		let running = self
			.running
			.take()
			.expect("start() can only be called once");

		if running.load(Ordering::Relaxed) {
			self.running = Some(running); // Put it back if already running
			return Ok(()); // Already running
		}
		running.store(true, Ordering::Relaxed);

		let id = self.id;
		let poll_interval = self.poll_interval;
		let engine = self
			.engine
			.take()
			.expect("start() can only be called once");
		let consume = self
			.consume
			.take()
			.expect("start() can only be called once");

		let handle = thread::spawn(move || {
			println!(
				"[Consumer {:?}] Started polling with interval {:?}",
				id, poll_interval
			);

			while running.load(Ordering::Relaxed) {
				if let Err(e) = Self::consume_events(
					id, &engine, &consume,
				) {
					eprintln!(
						"[Consumer {:?}] Error processing events: {}",
						id, e
					);
				}

				sleep(poll_interval);
			}

			println!("[Consumer {:?}] Stopped", id);
		});

		*self.handle.lock().unwrap() = Some(handle);
		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if let Some(ref running) = self.running {
			if !running.load(Ordering::Relaxed) {
				return Ok(()); // Already stopped
			}
			running.store(false, Ordering::Relaxed);
		} else {
			return Ok(()); // Already consumed by start()
		}

		if let Some(handle) = self.handle.lock().unwrap().take() {
			handle.join().expect("Failed to join consumer thread");
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running
			.as_ref()
			.map(|r| r.load(Ordering::Relaxed))
			.unwrap_or(false)
	}
}
