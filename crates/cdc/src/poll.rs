// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{
		Cdc, CdcChange, CdcConsumerId, CdcQueryTransaction, CommandTransaction, Engine as EngineInterface, Key,
		KeyKind,
	},
	key::{CdcConsumerKey, EncodableKey},
};
use reifydb_engine::StandardEngine;
use tokio::{task::JoinHandle, time::sleep};
use tracing::{debug, error};

use crate::{CdcCheckpoint, CdcConsume, CdcConsumer};

/// Configuration for a CDC poll consumer
#[derive(Debug, Clone)]
pub struct PollConsumerConfig {
	/// Unique identifier for this consumer
	pub consumer_id: CdcConsumerId,
	/// How often to poll for new CDC events
	pub poll_interval: Duration,
	/// Maximum batch size for fetching CDC events (None = unbounded)
	pub max_batch_size: Option<u64>,
}

impl PollConsumerConfig {
	pub fn new(consumer_id: CdcConsumerId, poll_interval: Duration, max_batch_size: Option<u64>) -> Self {
		Self {
			consumer_id,
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

/// Wait for watermark to reach target version with exponential backoff.
/// Returns Ok(safe_version) if watermark catches up, Err(()) on timeout.
async fn wait_for_watermark_with_backoff(
	engine: &StandardEngine,
	target_version: CommitVersion,
	total_timeout: Duration,
) -> Option<CommitVersion> {
	let start = std::time::Instant::now();
	let mut backoff_ms = 5;
	const MAX_BACKOFF_MS: u64 = 50;

	loop {
		// Check if watermark has reached target
		let done_until = engine.done_until();
		if done_until >= target_version {
			return Some(done_until);
		}

		if start.elapsed() >= total_timeout {
			return None;
		}

		// Sleep with current backoff
		sleep(Duration::from_millis(backoff_ms)).await;

		// Exponential backoff: 5ms -> 10ms -> 20ms -> 40ms -> 50ms (max)
		backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
	}
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
	async fn consume_batch(
		state: &ConsumerState,
		engine: &StandardEngine,
		consumer: &C,
		max_batch_size: Option<u64>,
	) -> Result<usize> {
		let current_version = engine.current_version().await?;

		let safe_version = match wait_for_watermark_with_backoff(
			engine,
			current_version,
			Duration::from_millis(200),
		)
		.await
		{
			Some(version) => version,
			None => {
				// Timeout - watermark hasn't caught up, skip this batch
				return Ok(0);
			}
		};

		let mut transaction = engine.begin_command().await?;

		let checkpoint = CdcCheckpoint::fetch(&mut transaction, &state.consumer_key).await?;

		// If safe_version <= checkpoint, there's nothing safe to fetch yet
		if safe_version <= checkpoint {
			transaction.rollback()?;
			return Ok(0);
		}

		// Only fetch CDC events up to safe_version to avoid race conditions
		let transactions = fetch_cdcs_until(&mut transaction, checkpoint, safe_version, max_batch_size).await?;

		if transactions.is_empty() {
			transaction.rollback()?;
			return Ok(0);
		}

		let count = transactions.len();
		let latest_version = transactions.iter().map(|tx| tx.version).max().unwrap_or(checkpoint);

		// Filter transactions to only those with Row or Flow-related changes
		// Flow-related changes are needed to detect new flow definitions
		let relevant_transactions = transactions
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

		if !relevant_transactions.is_empty() {
			consumer.consume(&mut transaction, relevant_transactions).await?;
		}

		CdcCheckpoint::persist(&mut transaction, &state.consumer_key, latest_version).await?;
		transaction.commit().await?;

		Ok(count)
	}

	async fn polling_loop(
		config: PollConsumerConfig,
		engine: StandardEngine,
		consumer: Box<C>,
		state: Arc<ConsumerState>,
	) {
		debug!("[Consumer {:?}] Started polling with interval {:?}", config.consumer_id, config.poll_interval);

		while state.running.load(Ordering::Acquire) {
			match Self::consume_batch(&state, &engine, &consumer, config.max_batch_size).await {
				Ok(count) if count > 0 => {
					// More events likely available - poll again immediately
				}
				Ok(_) => {
					sleep(config.poll_interval).await;
				}
				Err(error) => {
					error!("[Consumer {:?}] Error consuming events: {}", config.consumer_id, error);
					// Sleep before retrying on error
					sleep(config.poll_interval).await;
				}
			}
		}

		debug!("[Consumer {:?}] Stopped", config.consumer_id);
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

		self.worker = Some(tokio::spawn(Self::polling_loop(config, engine, consumer, state)));

		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.state.running.swap(false, Ordering::AcqRel) {
			return Ok(());
		}

		if let Some(worker) = self.worker.take() {
			// Abort the task - we don't need to wait for it to finish
			worker.abort();
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.state.running.load(Ordering::Acquire)
	}
}

async fn fetch_cdcs_until(
	txn: &mut impl CommandTransaction,
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
	let cdc = txn.begin_cdc_query().await?;
	let batch = cdc.range(Bound::Excluded(since_version), upper_bound).await?;
	Ok(batch.items)
}
