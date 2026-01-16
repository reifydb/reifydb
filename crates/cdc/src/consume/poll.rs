// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	thread::{JoinHandle, sleep},
	time::Duration,
};

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::cdc::{Cdc, CdcChange, CdcConsumerId},
	key::{EncodableKey, Key, cdc_consumer::CdcConsumerKey, kind::KeyKind},
};
use reifydb_type::Result;
use tracing::{debug, error};

use super::{
	checkpoint::CdcCheckpoint,
	consumer::{CdcConsume, CdcConsumer},
	host::CdcHost,
};
use crate::storage::CdcStore;

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

pub struct PollConsumer<H: CdcHost, F: CdcConsume> {
	host: Option<H>,
	consumer: Option<Box<F>>,
	config: PollConsumerConfig,
	state: Arc<ConsumerState>,
	worker: Option<JoinHandle<()>>,
	store: Option<CdcStore>,
}

struct ConsumerState {
	consumer_key: EncodedKey,
	running: AtomicBool,
}

impl<H: CdcHost, C: CdcConsume> PollConsumer<H, C> {
	pub fn new(config: PollConsumerConfig, host: H, consume: C, store: CdcStore) -> Self {
		let consumer_key = CdcConsumerKey {
			consumer: config.consumer_id.clone(),
		}
		.encode();

		Self {
			host: Some(host),
			consumer: Some(Box::new(consume)),
			config,
			state: Arc::new(ConsumerState {
				consumer_key,
				running: AtomicBool::new(false),
			}),
			worker: None,
			store: Some(store),
		}
	}

	/// Consume a batch of CDC events.
	/// Returns the number of CDC transactions processed (0 if none available).
	fn consume_batch(
		state: &ConsumerState,
		host: &H,
		consumer: &C,
		store: &CdcStore,
		max_batch_size: Option<u64>,
	) -> Result<usize> {
		// Get current version and wait for watermark to catch up.
		// This ensures we can fetch CDC events up to the latest committed version
		// rather than always chasing a lagging done_until.
		let current_version = host.current_version()?;
		host.wait_for_mark_timeout(current_version, Duration::from_millis(200));

		let safe_version = host.done_until();

		let mut transaction = host.begin_command()?;

		let checkpoint = CdcCheckpoint::fetch(&mut transaction, &state.consumer_key)?;
		if safe_version <= checkpoint {
			// there's nothing safe to fetch yet
			transaction.rollback()?;
			return Ok(0);
		}

		// Only fetch CDC events up to safe_version to avoid race conditions
		let transactions = fetch_cdcs_until(store, checkpoint, safe_version, max_batch_size)?;
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
		host: H,
		consumer: Box<C>,
		store: CdcStore,
		state: Arc<ConsumerState>,
	) {
		debug!("[Consumer {:?}] Started polling with interval {:?}", config.consumer_id, config.poll_interval);

		while state.running.load(Ordering::Acquire) {
			match Self::consume_batch(&state, &host, &consumer, &store, config.max_batch_size) {
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

impl<H: CdcHost, F: CdcConsume + Send + 'static> CdcConsumer for PollConsumer<H, F> {
	fn start(&mut self) -> Result<()> {
		assert!(self.worker.is_none(), "start() can only be called once");

		if self.state.running.swap(true, Ordering::AcqRel) {
			return Ok(());
		}

		let host = self.host.take().expect("host already consumed");
		let consumer = self.consumer.take().expect("consumer already consumed");
		let store = self.store.take().expect("cdc_store already consumed");

		let state = Arc::clone(&self.state);
		let config = self.config.clone();

		self.worker = Some(thread::Builder::new()
			.name(config.thread_name.clone())
			.spawn(move || {
				Self::polling_loop(config, host, consumer, store, state);
			})
			.expect("Failed to spawn CDC poll thread"));

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
	cdc_store: &CdcStore,
	since_version: CommitVersion,
	until_version: CommitVersion,
	max_batch_size: Option<u64>,
) -> Result<Vec<Cdc>> {
	let batch_size = max_batch_size.unwrap_or(1024);
	let batch = cdc_store.read_range(Bound::Excluded(since_version), Bound::Included(until_version), batch_size)?;
	Ok(batch.items)
}
