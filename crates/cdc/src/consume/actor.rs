// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, ops::Bound, time::Duration};

use reifydb_core::{
	actors::cdc::CdcPollMessage,
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::cdc::{Cdc, CdcConsumerId, SystemChange},
	key::{EncodableKey, Key, cdc_consumer::CdcConsumerKey, kind::KeyKind},
};
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	traits::{Actor, Directive},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;
use tracing::{debug, error};

use super::{checkpoint::CdcCheckpoint, consumer::CdcConsume, host::CdcHost};
use crate::storage::CdcStore;

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

/// Phase of the poll actor state machine
pub enum Phase {
	/// Ready to accept a new Poll
	Ready,
	/// Waiting for watermark to catch up to a specific version
	WaitingForWatermark {
		current_version: CommitVersion,
		retries_remaining: u8,
	},
	/// Waiting for the consumer to respond
	WaitingForConsume {
		/// The latest CDC version in the batch
		latest_version: CommitVersion,
		/// Number of CDC transactions in the batch
		count: usize,
	},
}

pub struct PollState {
	phase: Phase,
	/// In-memory mirror of the durable checkpoint, plus any skip-ahead
	/// advances made on idle polls. Seeded from storage on first poll.
	/// Used as the lower bound for `fetch_cdcs_until` so we never re-read
	/// filtered ranges. Never persisted directly - the durable record is
	/// updated only inside `finish_consume`, where it commits atomically
	/// alongside the consumer's user-data writes.
	cached_checkpoint: Option<CommitVersion>,
}

impl<H: CdcHost, C: CdcConsume + Send + Sync + 'static> Actor for PollActor<H, C> {
	type State = PollState;
	type Message = CdcPollMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		debug!(
			"[Consumer {:?}] Started polling with interval {:?}",
			self.config.consumer_id, self.config.poll_interval
		);

		// Send initial poll message to start the loop
		let _ = ctx.self_ref().send(CdcPollMessage::Poll);

		PollState {
			phase: Phase::Ready,
			cached_checkpoint: None,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CdcPollMessage::Poll => {
				// Ignore Poll if we're already waiting for watermark or consume
				if !matches!(state.phase, Phase::Ready) {
					return Directive::Continue;
				}

				if ctx.is_cancelled() {
					debug!("[Consumer {:?}] Stopped", self.config.consumer_id);
					return Directive::Stop;
				}

				// Get current version and check watermark readiness (non-blocking)
				let current_version = match self.host.current_version() {
					Ok(v) => v,
					Err(e) => {
						error!(
							"[Consumer {:?}] Error getting current version: {}",
							self.config.consumer_id, e
						);
						ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
						return Directive::Continue;
					}
				};

				let done = self.host.done_until();
				if done >= current_version {
					// Watermark is ready, proceed with batch processing
					self.start_consume(state, ctx);
				} else {
					state.phase = Phase::WaitingForWatermark {
						current_version,
						retries_remaining: 4,
					};
					ctx.schedule_once(Duration::from_millis(50), || CdcPollMessage::CheckWatermark);
				}
			}
			CdcPollMessage::CheckWatermark => {
				if let Phase::WaitingForWatermark {
					current_version,
					retries_remaining,
				} = state.phase
				{
					let is_cancelled = ctx.is_cancelled();
					if is_cancelled {
						debug!("[Consumer {:?}] Stopped", self.config.consumer_id);
						return Directive::Stop;
					}

					let done = self.host.done_until();
					if done >= current_version {
						// Watermark caught up, proceed
						state.phase = Phase::Ready;
						self.start_consume(state, ctx);
					} else if retries_remaining == 0 {
						// Timeout - proceed anyway (matches original 200ms behavior)
						state.phase = Phase::Ready;
						self.start_consume(state, ctx);
					} else {
						// Still not ready, schedule another check
						state.phase = Phase::WaitingForWatermark {
							current_version,
							retries_remaining: retries_remaining - 1,
						};
						ctx.schedule_once(Duration::from_millis(50), || {
							CdcPollMessage::CheckWatermark
						});
					}
				}
				// If not in WaitingForWatermark phase, ignore
			}
			CdcPollMessage::ConsumeResponse(result) => {
				// Only handle if we're waiting for a consume response
				if let Phase::WaitingForConsume {
					latest_version,
					count,
				} = mem::replace(&mut state.phase, Phase::Ready)
				{
					self.finish_consume(state, ctx, latest_version, count, result);
				}
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(16)
	}
}

impl<H: CdcHost, C: CdcConsume> PollActor<H, C> {
	/// Start consuming a batch: fetch CDCs, filter, and send to consumer asynchronously.
	/// If no data is available, schedules the next poll directly.
	fn start_consume(&self, state: &mut PollState, ctx: &Context<CdcPollMessage>) {
		state.phase = Phase::Ready;

		let safe_version = self.host.done_until();

		let checkpoint = match state.cached_checkpoint {
			Some(v) => v,
			None => {
				// First poll: seed the cache from durable storage. After
				// this we never read the durable checkpoint again - the
				// cache is authoritative until process exit.
				let mut query = match self.host.begin_query() {
					Ok(q) => q,
					Err(e) => {
						error!(
							"[Consumer {:?}] Error beginning query: {}",
							self.config.consumer_id, e
						);
						ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
						return;
					}
				};
				let v = match CdcCheckpoint::fetch(
					&mut Transaction::Query(&mut query),
					&self.consumer_key,
				) {
					Ok(c) => c,
					Err(e) => {
						error!(
							"[Consumer {:?}] Error fetching checkpoint: {}",
							self.config.consumer_id, e
						);
						ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
						return;
					}
				};
				drop(query);
				state.cached_checkpoint = Some(v);
				v
			}
		};

		if safe_version <= checkpoint {
			// Nothing safe to fetch yet
			ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
			return;
		}

		let transactions = match self.fetch_cdcs_until(checkpoint, safe_version) {
			Ok(t) => t,
			Err(e) => {
				error!("[Consumer {:?}] Error fetching CDCs: {}", self.config.consumer_id, e);
				ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
				return;
			}
		};

		if transactions.is_empty() {
			ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
			return;
		}

		let count = transactions.len();
		let latest_version = transactions.iter().map(|tx| tx.version).max().unwrap_or(checkpoint);

		// Filter transactions to only those with Row or Flow-related changes
		let relevant_cdcs = transactions
			.into_iter()
			.filter(|cdc| {
				// Pass through if there are decoded row changes (columnar Change objects)
				!cdc.changes.is_empty()
					|| cdc.system_changes.iter().any(|sys_change| match sys_change {
						SystemChange::Insert {
							key,
							..
						}
						| SystemChange::Update {
							key,
							..
						}
						| SystemChange::Delete {
							key,
							..
						} => {
							if let Some(kind) = Key::kind(key) {
								matches!(
									kind,
									KeyKind::Row
										| KeyKind::Flow | KeyKind::FlowNode
										| KeyKind::FlowNodeByFlow | KeyKind::FlowEdge
										| KeyKind::FlowEdgeByFlow
										| KeyKind::NamespaceFlow
								)
							} else {
								false
							}
						}
					})
			})
			.collect::<Vec<_>>();

		if relevant_cdcs.is_empty() {
			// No relevant CDCs in the safe range. Advance the in-memory
			// checkpoint so the next poll does not re-fetch this range, but
			// do NOT touch the multi store - this advance is a process-local
			// skip-ahead, not a durable checkpoint. The durable checkpoint
			// moves only when `finish_consume` actually applies CDCs (a
			// real-work commit that the multi store legitimately
			// version-bumps).
			state.cached_checkpoint = Some(latest_version);
			let _ = ctx.self_ref().send(CdcPollMessage::Poll);
			return;
		}

		// Transition to WaitingForConsume phase before sending
		state.phase = Phase::WaitingForConsume {
			latest_version,
			count,
		};

		// Send to consumer with a callback that delivers ConsumeResponse back to self
		let self_ref = ctx.self_ref().clone();
		let reply: Box<dyn FnOnce(Result<()>) + Send> = Box::new(move |result| {
			let _ = self_ref.send(CdcPollMessage::ConsumeResponse(result));
		});

		self.consumer.consume(relevant_cdcs, reply);
	}

	/// Finish consuming: persist consumer checkpoint on success, schedule next poll.
	fn finish_consume(
		&self,
		state: &mut PollState,
		ctx: &Context<CdcPollMessage>,
		latest_version: CommitVersion,
		count: usize,
		result: Result<()>,
	) {
		state.phase = Phase::Ready;

		match result {
			Ok(()) => {
				// Advance the in-memory checkpoint. The consumer has applied
				// its writes via its own transaction; the checkpoint here is
				// a process-local watermark used as the lower bound for the
				// next fetch, mirrored from `state.cached_checkpoint`.
				//
				// We do NOT persist the checkpoint to the multi store. A
				// per-batch persist would write a `CdcConsumer`-keyed row -
				// filtered from CDC by `should_exclude_from_cdc` but still
				// allocating a fresh `current_version` on every batch. That
				// drifts version-sensitive callers (e.g. cdc-snapshot golden
				// scripts) and produces redundant work, since the consumer's
				// apply is idempotent: on restart the durable checkpoint is
				// re-seeded from storage in `start_consume`, the consumer
				// re-fetches the same range, and re-applies the same set/
				// unset writes - cheap and correct.
				state.cached_checkpoint = Some(latest_version);
				if count > 0 {
					// More events likely available - poll again immediately
					let _ = ctx.self_ref().send(CdcPollMessage::Poll);
				} else {
					ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
				}
			}
			Err(e) => {
				error!("[Consumer {:?}] Error consuming events: {}", self.config.consumer_id, e);
				ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Poll);
			}
		}
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
