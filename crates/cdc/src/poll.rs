// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{JoinHandle, sleep},
	time::Duration,
};
use std::thread;
use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{Cdc, CdcChange, CdcConsumerId, Key, KeyKind},
	key::{CdcConsumerKey, EncodableKey},
};
use reifydb_engine::StandardEngine;
use reifydb_transaction::{StandardCommandTransaction, cdc::CdcQueryTransaction};
use tracing::{debug, error};

use crate::{CdcCheckpoint, CdcConsume, CdcConsumer};

/// Configuration for a CDC poll consumer
#[derive(Debug, Clone)]
pub struct PollConsumerConfig {
	/// Unique identifier for this consumer
	pub consumer_id: CdcConsumerId,
	/// Thread name for the poll worker
	pub thread_name: String,
	/// How often to poll for new CDC events
	pub poll_interval: Duration,
	/// Maximum batch size for fetching CDC events (None = unbounded)
	pub max_batch_size: Option<u64>,
}

impl PollConsumerConfig {
	pub fn new(
		consumer_id: CdcConsumerId,
		thread_name: impl Into<String>,
		poll_interval: Duration,
		max_batch_size: Option<u64>,
	) -> Self {
		Self {
			consumer_id,
			thread_name: thread_name.into(),
			poll_interval,
			max_batch_size,
		}
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

	/// Consume a batch of CDC events.
	/// Returns the number of CDC transactions processed (0 if none available).
	fn consume_batch(
		state: &ConsumerState,
		engine: &StandardEngine,
		consumer: &C,
		max_batch_size: Option<u64>,
	) -> Result<usize> {
		// Get current version and wait for watermark to catch up.
		// This ensures we can fetch CDC events up to the latest committed version
		// rather than always chasing a lagging done_until.
		let current_version = engine.current_version()?;
		engine.wait_for_mark_timeout(current_version, Duration::from_millis(200));

		let safe_version = engine.done_until();

		let mut transaction = engine.begin_command()?;

		let checkpoint = CdcCheckpoint::fetch(&mut transaction, &state.consumer_key)?;
		if safe_version <= checkpoint {
			// there's nothing safe to fetch yet
			transaction.rollback()?;
			return Ok(0);
		}

		// Only fetch CDC events up to safe_version to avoid race conditions
		let transactions = fetch_cdcs_until(&mut transaction, checkpoint, safe_version, max_batch_size)?;
		if transactions.is_empty() {
			transaction.rollback()?;
			return Ok(0);
		}

		let count = transactions.len();
		let latest_version = transactions.iter().map(|tx| tx.version).max().unwrap_or(checkpoint);

		// Filter transactions to only those with Row or Flow-related changes
		// Flow-related changes are needed to detect new flow definitions
		let relevant_cdcs = transactions
			.into_iter()
			.filter(|cdc| {
				cdc.changes.iter().any(|change| match &change.change {
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
						if let Some(kind) = Key::kind(key) {
							matches!(
								kind,
								KeyKind::Row
									| KeyKind::Flow | KeyKind::FlowNode
									| KeyKind::FlowNodeByFlow | KeyKind::FlowEdge
									| KeyKind::FlowEdgeByFlow | KeyKind::NamespaceFlow
							)
						} else {
							false
						}
					}
				})
			})
			.collect::<Vec<_>>();

		if !relevant_cdcs.is_empty() {
			consumer.consume(&mut transaction, relevant_cdcs)?;
		}

		CdcCheckpoint::persist(&mut transaction, &state.consumer_key, latest_version)?;
		transaction.commit()?;

		Ok(count)
	}

	fn polling_loop(
		config: PollConsumerConfig,
		engine: StandardEngine,
		consumer: Box<C>,
		state: Arc<ConsumerState>,
	) {
		debug!("[Consumer {:?}] Started polling with interval {:?}", config.consumer_id, config.poll_interval);

		while state.running.load(Ordering::Acquire) {
			match Self::consume_batch(&state, &engine, &consumer, config.max_batch_size) {
				Ok(count) if count > 0 => {
					// More events likely available - poll again immediately
				}
				Ok(_) => {
					sleep(config.poll_interval);
				}
				Err(error) => {
					error!("[Consumer {:?}] Error consuming events: {}", config.consumer_id, error);
					// Sleep before retrying on error
					sleep(config.poll_interval);
				}
			}
		}

		debug!("[Consumer {:?}] Stopped", config.consumer_id);
	}
}

impl<F: CdcConsume + Send +'static> CdcConsumer for PollConsumer<F> {
	fn start(&mut self) -> Result<()> {
		assert!(self.worker.is_none(), "start() can only be called once");

		if self.state.running.swap(true, Ordering::AcqRel) {
			return Ok(());
		}

		let engine = self.engine.take().expect("engine already consumed");

		let consumer = self.consumer.take().expect("consumer already consumed");

		let state = Arc::clone(&self.state);
		let config = self.config.clone();

		self.worker = Some(
			thread::Builder::new()
				.name(config.thread_name.clone())
				.spawn(move || {
					Self::polling_loop(config, engine, consumer, state);
				})
				.expect("Failed to spawn CDC poll thread"),
		);

		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.state.running.swap(false, Ordering::AcqRel) {
			return Ok(());
		}

		if let Some(worker) = self.worker.take() {
			// Wait for thread to finish
			let _ = worker.join();
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.state.running.load(Ordering::Acquire)
	}
}

fn fetch_cdcs_until(
	txn: &mut StandardCommandTransaction,
	since_version: CommitVersion,
	until_version: CommitVersion,
	max_batch_size: Option<u64>,
) -> Result<Vec<Cdc>> {
	let upper_bound = match max_batch_size {
		Some(size) => {
			let batch_limit = CommitVersion(since_version.0.saturating_add(size));
			Bound::Included(batch_limit.min(until_version))
		}
		None => Bound::Included(until_version),
	};
	let cdc = txn.begin_cdc_query()?;
	let batch = cdc.range(Bound::Excluded(since_version), upper_bound)?;
	Ok(batch.items)
}
