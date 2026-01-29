// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Poll consumer actor for CDC event processing.
//!
//! This module provides an actor-based implementation of CDC polling:
//! - [`PollActor`]: Actor that polls for CDC events
//! - [`PollMsg`]: Message type (just Poll)
//!
//! The actor uses self-messaging for the polling loop:
//! - `pre_start()` sends the initial `Poll` message
//! - On data available: send `Poll` immediately for continuous processing
//! - On no data/error: schedule delayed `Poll` via timer

use std::{ops::Bound, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::cdc::{Cdc, CdcChange, CdcConsumerId},
	key::{EncodableKey, Key, cdc_consumer::CdcConsumerKey, kind::KeyKind},
};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, ActorConfig, Flow},
};
use reifydb_type::Result;
use tracing::{debug, error};

use super::{checkpoint::CdcCheckpoint, consumer::CdcConsume, host::CdcHost};
use crate::storage::CdcStore;

/// Messages for the poll actor
#[derive(Clone)]
pub enum PollMsg {
	/// Trigger a poll for CDC events
	Poll,
}

/// Configuration for the poll actor
#[derive(Debug, Clone)]
pub struct PollActorConfig {
	/// Unique identifier for this consumer
	pub consumer_id: CdcConsumerId,
	/// How often to poll when no data is available
	pub poll_interval: Duration,
	/// Maximum batch size for fetching CDC events (None = unbounded)
	pub max_batch_size: Option<u64>,
}

/// Poll actor - polls for CDC events and processes them.
///
/// Uses self-messaging for the polling loop:
/// - On startup, sends initial Poll message
/// - After processing, either sends immediate Poll (more data likely) or schedules delayed Poll (no data or error)
pub struct PollActor<H: CdcHost, C: CdcConsume> {
	config: PollActorConfig,
	host: H,
	consumer: Box<C>,
	store: CdcStore,
	consumer_key: EncodedKey,
}

impl<H: CdcHost, C: CdcConsume> PollActor<H, C> {
	/// Create a new poll actor
	pub fn new(config: PollActorConfig, host: H, consumer: C, store: CdcStore) -> Self {
		let consumer_key = CdcConsumerKey {
			consumer: config.consumer_id.clone(),
		}
		.encode();

		Self {
			config,
			host,
			consumer: Box::new(consumer),
			store,
			consumer_key,
		}
	}
}

/// Actor state - minimal since most state is in the actor itself
pub struct PollState;

impl<H: CdcHost, C: CdcConsume + Send + Sync + 'static> Actor for PollActor<H, C> {
	type State = PollState;
	type Message = PollMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		PollState
	}

	fn pre_start(&self, _state: &mut Self::State, ctx: &Context<Self::Message>) {
		debug!(
			"[Consumer {:?}] Started polling with interval {:?}",
			self.config.consumer_id, self.config.poll_interval
		);

		// Send initial poll message to start the loop
		let _ = ctx.self_ref().send(PollMsg::Poll);
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Flow {
		match msg {
			PollMsg::Poll => {
				let is_cancelled = ctx.is_cancelled();
				// Check if we should stop
				if is_cancelled {
					debug!("[Consumer {:?}] Stopped", self.config.consumer_id);
					return Flow::Stop;
				}

				match self.consume_batch() {
					Ok(count) if count > 0 => {
						// More events likely available - poll again immediately
						let _ = ctx.self_ref().send(PollMsg::Poll);
					}
					Ok(_) => {
						// No data - schedule delayed poll
						ctx.schedule_once(self.config.poll_interval, PollMsg::Poll);
					}
					Err(e) => {
						error!(
							"[Consumer {:?}] Error consuming events: {}",
							self.config.consumer_id, e
						);
						// Sleep before retrying on error
						ctx.schedule_once(self.config.poll_interval, PollMsg::Poll);
					}
				}
			}
		}
		Flow::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(16)
	}
}

impl<H: CdcHost, C: CdcConsume> PollActor<H, C> {
	/// Consume a batch of CDC events.
	/// Returns the number of CDC transactions processed (0 if none available).
	fn consume_batch(&self) -> Result<usize> {
		// Get current version and wait for watermark to catch up.
		let current_version = self.host.current_version()?;
		self.host.wait_for_mark_timeout(current_version, Duration::from_millis(200));

		let safe_version = self.host.done_until();

		let mut transaction = self.host.begin_command()?;

		let checkpoint = CdcCheckpoint::fetch(&mut transaction, &self.consumer_key)?;
		if safe_version <= checkpoint {
			// there's nothing safe to fetch yet
			transaction.rollback()?;
			return Ok(0);
		}

		// Only fetch CDC events up to safe_version to avoid race conditions
		let transactions = self.fetch_cdcs_until(checkpoint, safe_version)?;
		if transactions.is_empty() {
			transaction.rollback()?;
			return Ok(0);
		}

		let count = transactions.len();
		let latest_version = transactions.iter().map(|tx| tx.version).max().unwrap_or(checkpoint);

		// Filter transactions to only those with Row or Flow-related changes
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
			self.consumer.consume(&mut transaction, relevant_cdcs)?;
		}

		CdcCheckpoint::persist(&mut transaction, &self.consumer_key, latest_version)?;
		transaction.commit()?;

		Ok(count)
	}

	fn fetch_cdcs_until(&self, since_version: CommitVersion, until_version: CommitVersion) -> Result<Vec<Cdc>> {
		let batch_size = self.config.max_batch_size.unwrap_or(1024);
		let batch = self.store.read_range(
			Bound::Excluded(since_version),
			Bound::Included(until_version),
			batch_size,
		)?;
		Ok(batch.items)
	}
}
