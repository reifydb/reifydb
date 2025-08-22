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
        CdcConsume, CdcConsumer, CdcEvent, CdcQueryTransaction,
        CommandTransaction, ConsumerId, Engine as EngineInterface, Key,
        Transaction, VersionedCommandTransaction,
        VersionedQueryTransaction,
        key::{CdcConsumerKey, EncodableKey},
        worker_pool::Priority,
	},
	log_debug, log_error,
	row::EncodedRow,
	util::CowVec,
};
use reifydb_core::interface::CdcTransaction;
use reifydb_engine::StandardEngine;

/// Configuration for a CDC poll consumer
#[derive(Debug, Clone)]
pub struct PollConsumerConfig {
	/// Unique identifier for this consumer
	pub consumer_id: ConsumerId,
	/// How often to poll for new CDC events
	pub poll_interval: Duration,
	/// Priority for the polling task in the worker pool
	pub priority: Priority,
}

impl PollConsumerConfig {
	pub fn new(consumer_id: ConsumerId, poll_interval: Duration) -> Self {
		Self {
			consumer_id,
			poll_interval,
			priority: Priority::Normal,
		}
	}

	pub fn with_priority(mut self, priority: Priority) -> Self {
		self.priority = priority;
		self
	}
}

pub struct PollConsumer<T: Transaction, F: CdcConsume<T>> {
	engine: Option<StandardEngine<T>>,
	consumer: Option<Box<F>>,
	config: PollConsumerConfig,
	state: Arc<ConsumerState>,
	worker: Option<JoinHandle<()>>,
}

struct ConsumerState {
	consumer_key: EncodedKey,
	running: AtomicBool,
}

impl<T: Transaction, C: CdcConsume<T>> PollConsumer<T, C> {
	pub fn new(
		config: PollConsumerConfig,
		engine: StandardEngine<T>,
		consume: C,
	) -> Self {
		let consumer_key = CdcConsumerKey {
			consumer: config.consumer_id.clone(),
		}
		.encode();

		Self {
			engine: Some(engine),
			consumer: Some(Box::new(consume)),
			config,
			state: Arc::new(ConsumerState {
				consumer_key,
				running: AtomicBool::new(false),
			}),
			worker: None,
		}
	}

	fn consume_batch(
		state: &ConsumerState,
		engine: &StandardEngine<T>,
		consumer: &C,
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
			consumer.consume(&mut transaction, table_events)?;
		}

		persist_checkpoint(
			&mut transaction,
			&state.consumer_key,
			latest_version,
		)?;
		transaction.commit().map(|_| ())
	}

	fn polling_loop(
		config: &PollConsumerConfig,
		engine: StandardEngine<T>,
		consumer: Box<C>,
		state: Arc<ConsumerState>,
	) {
		log_debug!(
			"[Consumer {:?}] Started polling with interval {:?}",
			config.consumer_id,
			config.poll_interval
		);

		while state.running.load(Ordering::Acquire) {
			if let Err(error) =
				Self::consume_batch(&state, &engine, &consumer)
			{
				log_error!(
					"[Consumer {:?}] Error consuming events: {}",
					config.consumer_id,
					error
				);
			}

			thread::sleep(config.poll_interval);
		}

		log_debug!("[Consumer {:?}] Stopped", config.consumer_id);
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

		let consumer = self
			.consumer
			.take()
			.expect("consumer already consumed");

		let state = Arc::clone(&self.state);
		let config = self.config.clone();

		self.worker = Some(thread::spawn(move || {
			Self::polling_loop(&config, engine, consumer, state);
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
	transaction: &mut CommandTransaction<T>,
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
	transaction: &mut CommandTransaction<T>,
	consumer_key: &EncodedKey,
	version: Version,
) -> Result<()> {
	let version_bytes = version.to_be_bytes().to_vec();
	transaction.set(consumer_key, EncodedRow(CowVec::new(version_bytes)))
}

fn fetch_events_since<T: Transaction>(
	transaction: &mut CommandTransaction<T>,
	since_version: Version,
) -> Result<Vec<CdcEvent>> {
	Ok(transaction
		.cdc()
		.begin_query()?
		.range(Bound::Excluded(since_version), Bound::Unbounded)?
		.collect())
}
