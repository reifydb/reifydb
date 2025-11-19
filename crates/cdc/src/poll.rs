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
	event::cdc::CdcCheckpointAdvancedEvent,
	interface::{
		Cdc, CdcChange, CdcConsumerId, CdcQueryTransaction, CommandTransaction, Engine as EngineInterface, Key,
		MultiVersionCommandTransaction, WithEventBus,
	},
	key::{CdcConsumerKey, EncodableKey},
	log_debug, log_error,
};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::Priority;

use crate::{CdcCheckpoint, CdcConsume, CdcConsumer};

/// Configuration for a CDC poll consumer
#[derive(Debug, Clone)]
pub struct PollConsumerConfig {
	/// Unique identifier for this consumer
	pub consumer_id: CdcConsumerId,
	/// How often to poll for new CDC events
	pub poll_interval: Duration,
	/// Priority for the polling task in the worker pool
	pub priority: Priority,
	/// Maximum batch size for fetching CDC events (None = unbounded)
	pub max_batch_size: Option<u64>,
}

impl PollConsumerConfig {
	pub fn new(consumer_id: CdcConsumerId, poll_interval: Duration, max_batch_size: Option<u64>) -> Self {
		Self {
			consumer_id,
			poll_interval,
			priority: Priority::Normal,
			max_batch_size,
		}
	}

	pub fn with_priority(mut self, priority: Priority) -> Self {
		self.priority = priority;
		self
	}
}

pub struct PollConsumer<F: CdcConsume> {
	engine: Option<StandardEngine>,
	consumer: Option<Box<F>>,
	config: PollConsumerConfig,
	state: Arc<ConsumerState>,
	worker: Option<JoinHandle<()>>,
}

struct ConsumerState {
	consumer_key: EncodedKey,
	running: AtomicBool,
}

impl<C: CdcConsume> PollConsumer<C> {
	pub fn new(config: PollConsumerConfig, engine: StandardEngine, consume: C) -> Self {
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
		engine: &StandardEngine,
		consumer: &C,
		consumer_id: &CdcConsumerId,
		max_batch_size: Option<u64>,
	) -> Result<Option<(CommitVersion, u64)>> {
		let mut transaction = engine.begin_command()?;

		let checkpoint = CdcCheckpoint::fetch(&mut transaction, &state.consumer_key)?;

		let transactions = fetch_cdcs_since(&mut transaction, checkpoint, max_batch_size)?;
		if transactions.is_empty() {
			transaction.rollback()?;
			return Ok(None);
		}

		let latest_version = transactions.iter().map(|tx| tx.version).max().unwrap_or(checkpoint);

		// Filter transactions to only those with Row changes
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
		let current_version = transaction.commit()?;

		engine.event_bus().emit(CdcCheckpointAdvancedEvent {
			consumer_id: consumer_id.clone(),
			version: latest_version,
		});

		let lag = current_version.0.saturating_sub(latest_version.0);

		Ok(Some((latest_version, lag)))
	}

	fn polling_loop(
		config: &PollConsumerConfig,
		engine: StandardEngine,
		consumer: Box<C>,
		state: Arc<ConsumerState>,
	) {
		log_debug!(
			"[Consumer {:?}] Started polling with interval {:?}",
			config.consumer_id,
			config.poll_interval
		);

		while state.running.load(Ordering::Acquire) {
			match Self::consume_batch(
				&state,
				&engine,
				&consumer,
				&config.consumer_id,
				config.max_batch_size,
			) {
				Ok(Some((_processed_version, _lag))) => {
					// FIXME log this
				}
				Ok(None) => {}
				Err(error) => {
					log_error!(
						"[Consumer {:?}] Error consuming events: {}",
						config.consumer_id,
						error
					);
				}
			}
		}

		log_debug!("[Consumer {:?}] Stopped", config.consumer_id);
	}
}

impl<F: CdcConsume> CdcConsumer for PollConsumer<F> {
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

fn fetch_cdcs_since(
	txn: &mut impl CommandTransaction,
	since_version: CommitVersion,
	max_batch_size: Option<u64>,
) -> Result<Vec<Cdc>> {
	let upper_bound = match max_batch_size {
		Some(size) => Bound::Excluded(CommitVersion(since_version.0.saturating_add(size).saturating_add(1))),
		None => Bound::Unbounded,
	};
	txn.with_cdc_query(|cdc| Ok(cdc.range(Bound::Excluded(since_version), upper_bound)?.collect::<Vec<_>>()))
}
