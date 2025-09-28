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
	CommitVersion, EncodedKey, Result,
	interface::{
		Cdc, CdcChange, CdcQueryTransaction, CommandTransaction, ConsumerId, Engine as EngineInterface, Key,
		MultiVersionCommandTransaction, Transaction,
		key::{CdcConsumerKey, EncodableKey},
	},
	log_debug, log_error,
};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::Priority;

use crate::{CdcCheckpoint, CdcConsume, CdcConsumer};

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
	pub fn new(config: PollConsumerConfig, engine: StandardEngine<T>, consume: C) -> Self {
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

	fn consume_batch(state: &ConsumerState, engine: &StandardEngine<T>, consumer: &C) -> Result<()> {
		let mut transaction = engine.begin_command()?;

		let checkpoint = CdcCheckpoint::fetch(&mut transaction, &state.consumer_key)?;

		let transactions = fetch_cdcs_since(&mut transaction, checkpoint)?;
		if transactions.is_empty() {
			return transaction.rollback();
		}

		let latest_version = transactions.iter().map(|tx| tx.version).max().unwrap_or(checkpoint);

		let row_transactions = transactions
			.into_iter()
			.filter(|tx| {
				tx.changes.iter().any(|change| match &change.change {
					CdcChange::Insert {
						key,
						..
					}
					| CdcChange::Update {
						key,
						..
					}
					| CdcChange::Delete {
						key,
						..
					} => {
						matches!(Key::decode(key), Some(Key::Row(_)))
					}
				})
			})
			.collect::<Vec<_>>();

		if !row_transactions.is_empty() {
			consumer.consume(&mut transaction, row_transactions)?;
		}

		CdcCheckpoint::persist(&mut transaction, &state.consumer_key, latest_version)?;
		transaction.commit()?;
		Ok(())
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

		let mut iteration = 0u64;
		while state.running.load(Ordering::Acquire) {
			iteration += 1;
			if let Err(error) = Self::consume_batch(&state, &engine, &consumer) {
				log_error!("[Consumer {:?}] Error consuming events: {}", config.consumer_id, error);
			}

			thread::sleep(config.poll_interval);
		}
	}
}

impl<T: Transaction + 'static, F: CdcConsume<T>> CdcConsumer for PollConsumer<T, F> {
	fn start(&mut self) -> Result<()> {
		assert!(self.worker.is_none(), "start() can only be called once");

		if self.state.running.swap(true, Ordering::AcqRel) {
			return Ok(());
		}

		let engine = self.engine.take().expect("engine already consumed");

		let consumer = self.consumer.take().expect("consumer already consumed");

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

fn fetch_cdcs_since(txn: &mut impl CommandTransaction, since_version: CommitVersion) -> Result<Vec<Cdc>> {
	txn.with_cdc_query(|cdc| Ok(cdc.range(Bound::Excluded(since_version), Bound::Unbounded)?.collect::<Vec<_>>()))
}
