// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	backtrace::Backtrace,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::cdc::CdcPollMessage,
	common::CommitVersion,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		cdc::{Cdc, CdcConsumerId, CheckpointState},
	},
	key::{EncodableKey, cdc_consumer::CdcConsumerKey},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	fatal::{
		fatal,
		report::{FatalKind, FatalReport},
	},
};
use reifydb_transaction::{error::TransactionError, transaction::Transaction};
use reifydb_value::{Result, error::Error, reifydb_assertions, value::duration::Duration};
use tracing::{debug, error};

use super::{
	checkpoint::{CdcCheckpoint, CheckpointRow},
	consumer::CdcConsume,
	host::CdcHost,
	is_relevant_cdc,
	watermark::CdcConsumerWatermark,
};
use crate::storage::CdcStore;

#[derive(Debug, Clone)]
pub struct PollActorConfig {
	pub consumer_id: CdcConsumerId,

	pub poll_interval: Duration,

	pub max_batch_size: Option<u64>,
}

pub struct PollActor<H: CdcHost, C: CdcConsume> {
	config: PollActorConfig,
	host: H,
	consumer: Box<C>,
	store: CdcStore,
	consumer_key: EncodedKey,
	consumer_watermark: Option<CdcConsumerWatermark>,
	wake_armed: Arc<AtomicBool>,
}

impl<H: CdcHost, C: CdcConsume> PollActor<H, C> {
	pub fn new(
		config: PollActorConfig,
		host: H,
		consumer: C,
		store: CdcStore,
		consumer_watermark: Option<CdcConsumerWatermark>,
		wake_armed: Arc<AtomicBool>,
	) -> Self {
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
			consumer_watermark,
			wake_armed,
		}
	}

	#[inline]
	fn publish_watermark(&self, version: CommitVersion) {
		if let Some(wm) = &self.consumer_watermark {
			wm.store(version);
		}
	}

	#[inline]
	fn consume_wait_timeout(&self) -> Duration {
		self.host.catalog().get_config_duration(ConfigKey::CdcConsumeWaitTimeout)
	}
}

pub enum Phase {
	Ready,

	WaitingForWatermark,

	WaitingForConsume {
		latest_version: CommitVersion,

		count: usize,

		generation: u64,
	},

	WaitingForResync {
		generation: u64,
	},
}

pub struct PollState {
	phase: Phase,

	cached_checkpoint: Option<CommitVersion>,

	consume_generation: u64,

	consume_stall_ticks: u32,
}

impl<H: CdcHost, C: CdcConsume + Send + Sync + 'static> Actor for PollActor<H, C> {
	type State = PollState;
	type Message = CdcPollMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		debug!(
			"[Consumer {:?}] Started polling with interval {:?}",
			self.config.consumer_id, self.config.poll_interval
		);

		let _ = ctx.self_ref().send(CdcPollMessage::Poll);
		let _ = ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Tick);

		PollState {
			phase: Phase::Ready,
			cached_checkpoint: None,
			consume_generation: 0,
			consume_stall_ticks: 0,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CdcPollMessage::Poll => self.on_poll(state, ctx),
			CdcPollMessage::CheckWatermark => self.on_check_watermark(state, ctx),
			CdcPollMessage::ConsumeResponse {
				generation,
				result,
			} => self.on_consume_response(state, ctx, generation, result),
			CdcPollMessage::ResyncResponse {
				generation,
				result,
			} => self.on_resync_response(state, ctx, generation, result),
			CdcPollMessage::Tick => self.on_tick(state, ctx),
			CdcPollMessage::Shutdown => {
				debug!("[Consumer {:?}] Shutdown", self.config.consumer_id);
				Directive::Stop
			}
		}
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl<H: CdcHost, C: CdcConsume> PollActor<H, C> {
	#[inline]
	fn on_poll(&self, state: &mut PollState, ctx: &Context<CdcPollMessage>) -> Directive {
		if !matches!(state.phase, Phase::Ready) {
			return Directive::Continue;
		}
		if ctx.is_cancelled() {
			debug!("[Consumer {:?}] Stopped", self.config.consumer_id);
			return Directive::Stop;
		}
		let current_version = match self.host.current_version() {
			Ok(v) => v,
			Err(e) => {
				error!("[Consumer {:?}] Error getting current version: {}", self.config.consumer_id, e);
				return Directive::Continue;
			}
		};
		if self.host.done_until() >= current_version {
			self.start_consume(state, ctx);
		} else {
			state.phase = Phase::WaitingForWatermark;
			let self_ref = ctx.self_ref();
			self.host.notify_on_mark(
				current_version,
				Box::new(move || {
					let _ = self_ref.send(CdcPollMessage::CheckWatermark);
				}),
			);
		}
		Directive::Continue
	}

	#[inline]
	fn on_check_watermark(&self, state: &mut PollState, ctx: &Context<CdcPollMessage>) -> Directive {
		if !matches!(state.phase, Phase::WaitingForWatermark) {
			return Directive::Continue;
		}
		if ctx.is_cancelled() {
			debug!("[Consumer {:?}] Stopped", self.config.consumer_id);
			return Directive::Stop;
		}
		state.phase = Phase::Ready;
		self.start_consume(state, ctx);
		Directive::Continue
	}

	#[inline]
	fn on_consume_response(
		&self,
		state: &mut PollState,
		ctx: &Context<CdcPollMessage>,
		generation: u64,
		result: Result<()>,
	) -> Directive {
		if let Phase::WaitingForConsume {
			latest_version,
			count,
			generation: pending,
		} = state.phase
		{
			if pending != generation {
				return Directive::Continue;
			}
			state.phase = Phase::Ready;
			self.finish_consume(state, ctx, latest_version, count, result);
		}
		Directive::Continue
	}

	#[inline]
	fn on_tick(&self, state: &mut PollState, ctx: &Context<CdcPollMessage>) -> Directive {
		if ctx.is_cancelled() {
			debug!("[Consumer {:?}] Stopped", self.config.consumer_id);
			return Directive::Stop;
		}

		let flow = if matches!(state.phase, Phase::Ready) {
			self.on_poll(state, ctx)
		} else if matches!(state.phase, Phase::WaitingForWatermark) {
			self.on_check_watermark(state, ctx)
		} else {
			self.check_consume_stall(state, ctx)
		};
		if matches!(flow, Directive::Stop) {
			return Directive::Stop;
		}

		ctx.schedule_once(self.config.poll_interval, || CdcPollMessage::Tick);
		Directive::Continue
	}

	#[inline]
	fn check_consume_stall(&self, state: &mut PollState, ctx: &Context<CdcPollMessage>) -> Directive {
		if !matches!(state.phase, Phase::WaitingForConsume { .. } | Phase::WaitingForResync { .. }) {
			return Directive::Continue;
		}
		state.consume_stall_ticks = state.consume_stall_ticks.saturating_add(1);
		if state.consume_stall_ticks < self.stall_tick_threshold() {
			return Directive::Continue;
		}
		error!(
			"[Consumer {:?}] consume reply not received within {:?}; re-dispatching batch",
			self.config.consumer_id,
			self.consume_wait_timeout()
		);
		state.phase = Phase::Ready;
		state.consume_stall_ticks = 0;
		let _ = ctx.self_ref().send(CdcPollMessage::Poll);
		Directive::Continue
	}

	#[inline]
	fn stall_tick_threshold(&self) -> u32 {
		let consume_ms = self.consume_wait_timeout().to_std().as_millis().max(1);
		let poll_ms = self.config.poll_interval.to_std().as_millis().max(1);
		consume_ms.div_ceil(poll_ms).max(1) as u32
	}

	fn start_consume(&self, state: &mut PollState, ctx: &Context<CdcPollMessage>) {
		state.phase = Phase::Ready;
		self.wake_armed.store(false, Ordering::Release);
		let safe_version = self.host.cdc_producer_watermark();
		if safe_version > self.host.done_until() {
			return;
		}

		let Some(checkpoint) = self.resolve_checkpoint(state, ctx) else {
			return;
		};
		let truncated_before = match self.store.truncated_before() {
			Ok(v) => v,
			Err(e) => {
				error!(
					"[Consumer {:?}] Error reading truncation floor: {}",
					self.config.consumer_id, e
				);
				return;
			}
		};
		if checkpoint.0.saturating_add(1) < truncated_before.0 {
			self.begin_resync(state, ctx, checkpoint, truncated_before);
			return;
		}
		if safe_version <= checkpoint {
			return;
		}

		let Some(transactions) = self.fetch_cdcs(checkpoint, safe_version) else {
			return;
		};
		if transactions.is_empty() {
			self.advance_checkpoint_skip_ahead(state, ctx, safe_version);
			return;
		}

		let (count, latest_version) = summarize_batch(checkpoint, &transactions);
		let relevant_cdcs: Vec<Cdc> = transactions.into_iter().filter(is_relevant_cdc).collect();

		if relevant_cdcs.is_empty() {
			self.advance_checkpoint_skip_ahead(state, ctx, latest_version);
			return;
		}

		state.consume_generation = state.consume_generation.wrapping_add(1);
		let generation = state.consume_generation;
		state.phase = Phase::WaitingForConsume {
			latest_version,
			count,
			generation,
		};
		state.consume_stall_ticks = 0;
		self.dispatch_to_consumer(relevant_cdcs, generation, ctx);
	}

	#[inline]
	fn advance_checkpoint_skip_ahead(
		&self,
		state: &mut PollState,
		ctx: &Context<CdcPollMessage>,
		latest_version: CommitVersion,
	) {
		reifydb_assertions! {
			if let Some(prev) = state.cached_checkpoint {
				assert!(
					latest_version >= prev,
					"the consumer checkpoint moved backwards, so CDC that was already consumed would be \
					 re-delivered (cached checkpoint prev={}, new latest={})",
					prev.0,
					latest_version.0
				);
			}
		}
		state.cached_checkpoint = Some(latest_version);
		self.publish_watermark(latest_version);
		let _ = ctx.self_ref().send(CdcPollMessage::Poll);
	}

	#[inline]
	fn resolve_checkpoint(&self, state: &mut PollState, ctx: &Context<CdcPollMessage>) -> Option<CommitVersion> {
		if let Some(v) = state.cached_checkpoint {
			return Some(v);
		}
		let row = self.seed_checkpoint_from_durable()?;
		if let Some(row) = &row
			&& row.state == CheckpointState::Invalidated
		{
			let truncated = self.store.truncated_before().unwrap_or(row.version);
			self.begin_resync(state, ctx, row.version, truncated);
			return None;
		}
		let v = match row {
			Some(r) => r.version,
			None => {
				let floor = self.store.truncated_before().unwrap_or(CommitVersion(0));
				CommitVersion(floor.0.saturating_sub(1).max(1))
			}
		};
		state.cached_checkpoint = Some(v);
		self.publish_watermark(v);
		Some(v)
	}

	#[inline]
	fn seed_checkpoint_from_durable(&self) -> Option<Option<CheckpointRow>> {
		let mut query = match self.host.begin_query() {
			Ok(q) => q,
			Err(e) => {
				error!("[Consumer {:?}] Error beginning query: {}", self.config.consumer_id, e);
				return None;
			}
		};
		let row = match CdcCheckpoint::fetch_row(&mut Transaction::Query(&mut query), &self.consumer_key) {
			Ok(c) => c,
			Err(e) => {
				error!("[Consumer {:?}] Error fetching checkpoint: {}", self.config.consumer_id, e);
				return None;
			}
		};
		drop(query);
		Some(row)
	}

	#[inline]
	fn fetch_cdcs(&self, checkpoint: CommitVersion, safe_version: CommitVersion) -> Option<Vec<Cdc>> {
		match self.fetch_cdcs_until(checkpoint, safe_version) {
			Ok(t) => Some(t),
			Err(e) => {
				error!("[Consumer {:?}] Error fetching CDCs: {}", self.config.consumer_id, e);
				None
			}
		}
	}

	#[inline]
	fn dispatch_to_consumer(&self, cdcs: Vec<Cdc>, generation: u64, ctx: &Context<CdcPollMessage>) {
		let self_ref = ctx.self_ref().clone();
		let reply: Box<dyn FnOnce(Result<()>) + Send> = Box::new(move |result| {
			let _ = self_ref.send(CdcPollMessage::ConsumeResponse {
				generation,
				result,
			});
		});
		self.consumer.consume(cdcs, reply);
	}

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
			Ok(()) => self.advance_after_success(state, ctx, latest_version, count),
			Err(e) if TransactionError::is_consumer_overtaken(&e)
				|| TransactionError::is_snapshot_evicted(&e) =>
			{
				let cursor = state.cached_checkpoint.unwrap_or(CommitVersion(0));
				let truncated = self.store.truncated_before().unwrap_or(cursor);
				error!(
					"[Consumer {:?}] batch reads lost their MVCC history ({}); starting resync",
					self.config.consumer_id, e
				);
				self.begin_resync(state, ctx, cursor, truncated);
			}
			Err(e) => self.abort_on_error(e),
		}
	}

	fn begin_resync(
		&self,
		state: &mut PollState,
		ctx: &Context<CdcPollMessage>,
		cursor: CommitVersion,
		truncated_before: CommitVersion,
	) {
		error!(
			"[Consumer {:?}] overtaken by retention: cursor {} is behind the truncation floor {}",
			self.config.consumer_id, cursor.0, truncated_before.0
		);
		self.invalidate_durable_checkpoint();
		state.consume_generation = state.consume_generation.wrapping_add(1);
		let generation = state.consume_generation;
		state.phase = Phase::WaitingForResync {
			generation,
		};
		state.consume_stall_ticks = 0;
		let self_ref = ctx.self_ref().clone();
		let reply: Box<dyn FnOnce(Result<CommitVersion>) + Send> = Box::new(move |result| {
			let _ = self_ref.send(CdcPollMessage::ResyncResponse {
				generation,
				result,
			});
		});
		self.consumer.overtaken(cursor, truncated_before, reply);
	}

	#[inline]
	fn invalidate_durable_checkpoint(&self) {
		let result = self.host.begin_command().and_then(|mut txn| {
			CdcCheckpoint::invalidate(&mut txn, &self.consumer_key)?;
			txn.commit()
		});
		if let Err(e) = result {
			error!("[Consumer {:?}] Error invalidating checkpoint: {}", self.config.consumer_id, e);
		}
	}

	fn on_resync_response(
		&self,
		state: &mut PollState,
		ctx: &Context<CdcPollMessage>,
		generation: u64,
		result: Result<CommitVersion>,
	) -> Directive {
		let Phase::WaitingForResync {
			generation: pending,
		} = state.phase
		else {
			return Directive::Continue;
		};
		if pending != generation {
			return Directive::Continue;
		}
		state.phase = Phase::Ready;
		match result {
			Ok(resume) => {
				reifydb_assertions! {
					let floor = self.store.truncated_before().map(|v| v.0).unwrap_or(0);
					assert!(
						resume.0.saturating_add(1) >= floor,
						"a resync must resume at or past the truncation floor, or the next poll \
						 detects the same gap and the consumer loops forever (resume={}, floor={})",
						resume.0,
						floor
					);
				}
				state.cached_checkpoint = Some(resume);
				self.publish_watermark(resume);
				let _ = ctx.self_ref().send(CdcPollMessage::Poll);
				Directive::Continue
			}
			Err(e) => self.abort_on_error(e),
		}
	}

	#[inline]
	fn advance_after_success(
		&self,
		state: &mut PollState,
		ctx: &Context<CdcPollMessage>,
		latest_version: CommitVersion,
		count: usize,
	) {
		reifydb_assertions! {
			if let Some(prev) = state.cached_checkpoint {
				assert!(
					latest_version >= prev,
					"the consumer checkpoint moved backwards, so CDC that was already consumed would be \
					 re-delivered (cached checkpoint prev={}, new latest={})",
					prev.0,
					latest_version.0
				);
			}
		}
		state.cached_checkpoint = Some(latest_version);
		self.publish_watermark(latest_version);
		if count > 0 {
			let _ = ctx.self_ref().send(CdcPollMessage::Poll);
		}
	}

	#[inline]
	fn abort_on_error(&self, err: Error) -> ! {
		fatal(FatalReport::new(FatalKind::Error, format!("{}", err))
			.component("cdc consumer")
			.with("consumer", format!("{:?}", self.config.consumer_id))
			.backtrace(Backtrace::force_capture().to_string()))
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

#[inline]
fn summarize_batch(checkpoint: CommitVersion, transactions: &[Cdc]) -> (usize, CommitVersion) {
	let count = transactions.len();
	let latest_version = transactions.iter().map(|tx| tx.version).max().unwrap_or(checkpoint);
	(count, latest_version)
}
