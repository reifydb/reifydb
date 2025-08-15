// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
	time::Duration,
};

use reifydb_core::{
	EncodedKey, Result, Version,
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
use reifydb_engine::StandardEngine;

pub struct PollConsumer<T: Transaction, F: CdcConsume<T>> {
	engine: Option<StandardEngine<T>>,
	processor: Option<Box<F>>,
	state: Arc<ConsumerState>,
	worker: Option<JoinHandle<()>>,
}

struct ConsumerState {
	id: ConsumerId,
	interval: Duration,
	consumer_key: EncodedKey,
	running: AtomicBool,
}

impl<T: Transaction, F: CdcConsume<T>> PollConsumer<T, F> {
	pub fn new(
		id: ConsumerId,
		poll_interval: Duration,
		engine: StandardEngine<T>,
		consume: F,
	) -> Self {
		Self {
			engine: Some(engine),
			processor: Some(Box::new(consume)),
			state: Arc::new(ConsumerState {
				id: id.clone(),
				interval: poll_interval,
				consumer_key: CdcConsumerKey {
					consumer: id,
				}
				.encode(),
				running: AtomicBool::new(false),
			}),
			worker: None,
		}
	}

	fn process_batch(
		state: &ConsumerState,
		engine: &StandardEngine<T>,
		processor: &F,
	) -> Result<()> {
		let mut transaction = engine.begin_command()?;

		let checkpoint = fetch_checkpoint(
			&mut transaction,
			&state.consumer_key,
		)?;
		let events = fetch_events_since(&mut transaction, checkpoint)?;

		if events.is_empty() {
			return transaction.rollback();
		}

		let latest_version = events
			.iter()
			.map(|event| event.version)
			.max()
			.unwrap_or(checkpoint);

		let table_events = events
			.into_iter()
			.filter(|event| {
				matches!(
					Key::decode(event.key()),
					Some(Key::TableRow(_))
				)
			})
			.collect::<Vec<_>>();

		if !table_events.is_empty() {
			processor.consume(&mut transaction, table_events)?;
		}

		persist_checkpoint(
			&mut transaction,
			&state.consumer_key,
			latest_version,
		)?;
		transaction.commit()
	}

	fn polling_loop(
		engine: StandardEngine<T>,
		processor: Box<F>,
		state: Arc<ConsumerState>,
	) {
		println!(
			"[Consumer {:?}] Started polling with interval {:?}",
			state.id, state.interval
		);

		while state.running.load(Ordering::Acquire) {
			if let Err(error) =
				Self::process_batch(&state, &engine, &processor)
			{
				eprintln!(
					"[Consumer {:?}] Error processing events: {}",
					state.id, error
				);
			}

			thread::sleep(state.interval);
		}

		println!("[Consumer {:?}] Stopped", state.id);
	}
}

impl<T: Transaction + 'static, F: CdcConsume<T>> CdcConsumer
	for PollConsumer<T, F>
{
	fn start(&mut self) -> Result<()> {
		assert!(
			self.worker.is_none(),
			"start() can only be called once"
		);

		if self.state.running.swap(true, Ordering::AcqRel) {
			return Ok(());
		}

		let engine =
			self.engine.take().expect("engine already consumed");
		let processor = self
			.processor
			.take()
			.expect("processor already consumed");
		let state = Arc::clone(&self.state);

		self.worker = Some(thread::spawn(move || {
			Self::polling_loop(engine, processor, state);
		}));

		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.state.running.swap(false, Ordering::AcqRel) {
			return Ok(());
		}

		if let Some(worker) = self.worker.take() {
			worker.join().expect("Failed to join consumer thread");
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.state.running.load(Ordering::Acquire)
	}
}

fn fetch_checkpoint<T: Transaction>(
	transaction: &mut ActiveCommandTransaction<T>,
	consumer_key: &EncodedKey,
) -> Result<Version> {
	transaction
		.get(consumer_key)?
		.and_then(|record| {
			if record.row.len() >= 8 {
				let mut buffer = [0u8; 8];
				buffer.copy_from_slice(&record.row[0..8]);
				Some(u64::from_be_bytes(buffer))
			} else {
				None
			}
		})
		.map(Ok)
		.unwrap_or(Ok(1))
}

fn persist_checkpoint<T: Transaction>(
	transaction: &mut ActiveCommandTransaction<T>,
	consumer_key: &EncodedKey,
	version: Version,
) -> Result<()> {
	let version_bytes = version.to_be_bytes().to_vec();
	transaction.set(consumer_key, EncodedRow(CowVec::new(version_bytes)))
}

fn fetch_events_since<T: Transaction>(
	transaction: &mut ActiveCommandTransaction<T>,
	since_version: Version,
) -> Result<Vec<CdcEvent>> {
	Ok(transaction
		.cdc()
		.range(
			Bound::Excluded(since_version),
			Bound::Included(since_version + 1),
		)?
		.collect())
}
