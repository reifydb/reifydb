// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	backtrace::Backtrace,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	commit::{
		loader::CommitLoaderMessage,
		queue::{CommitPull, CommitQueue, CommitQueueFloor, lsn_through},
		watermark::ReaderWatermark,
	},
	common::CommitVersion,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		commit::{CommitEntry, Loaded},
	},
	internal_error,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::clock::{Clock, Instant},
	fatal::{
		fatal,
		report::{FatalKind, FatalReport},
	},
};
use reifydb_transaction::error::TransactionError;
use reifydb_value::{Result, byte_size::ByteSize, error::Error, value::duration::Duration};
use reifydb_wal::lsn::Lsn;

use crate::consumer::SubscriptionCommitLogConsumer;

const TICK_INTERVAL_MS: i64 = 10;

pub enum SubscriptionReaderMessage {
	Wake,
	Tick,
	Loaded {
		outcome: Result<Loaded>,
	},
	Dispatched {
		result: Result<()>,
	},
	Terminated {
		result: Result<()>,
	},
	Shutdown,
}

enum Phase {
	Ready,
	Loading,
	Dispatching {
		to: CommitVersion,
		lsn: Lsn,
	},
	Terminating {
		resume: CommitVersion,
		lsn: Lsn,
	},
}

pub struct SubscriptionReaderState {
	cursor: CommitVersion,
	lsn: Lsn,
	phase: Phase,
	waiting_since: Option<Instant>,
	replied: Arc<AtomicBool>,
}

pub struct SubscriptionReader {
	engine: StandardEngine,
	queue: CommitQueue,
	floor: CommitQueueFloor,
	consumer: SubscriptionCommitLogConsumer,
	watermark: ReaderWatermark,
	clock: Clock,
	start: CommitVersion,
	start_lsn: Lsn,
	loader: ActorRef<CommitLoaderMessage>,
}

impl SubscriptionReader {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		engine: StandardEngine,
		queue: CommitQueue,
		floor: CommitQueueFloor,
		consumer: SubscriptionCommitLogConsumer,
		watermark: ReaderWatermark,
		start: CommitVersion,
		start_lsn: Lsn,
		loader: ActorRef<CommitLoaderMessage>,
	) -> Self {
		let clock = engine.clock().clone();
		Self {
			engine,
			queue,
			floor,
			consumer,
			watermark,
			clock,
			start,
			start_lsn,
			loader,
		}
	}

	fn budget(&self) -> ByteSize {
		ByteSize::from_bytes(self.engine.catalog().get_config_uint8(ConfigKey::FlowPullBatchBytes))
	}

	fn load_budget(&self) -> ByteSize {
		ByteSize::from_bytes(self.engine.catalog().get_config_uint8(ConfigKey::SubscriptionLoadBatchBytes))
	}

	fn wait_timeout(&self) -> Duration {
		self.engine.catalog().get_config_duration(ConfigKey::SubscriptionWaitTimeout)
	}

	fn advance(&self, state: &mut SubscriptionReaderState, ctx: &Context<SubscriptionReaderMessage>) {
		if !matches!(state.phase, Phase::Ready) {
			return;
		}
		self.floor.disarm();
		let frontier = self.queue.released();
		let released = frontier.0;
		let done_until = self.engine.done_until();
		if released > done_until {
			let self_ref = ctx.self_ref().clone();
			self.engine.notify_on_mark(
				released,
				Box::new(move || {
					let _ = self_ref.send(SubscriptionReaderMessage::Wake);
				}),
			);
		}
		let bound = released.min(done_until);
		if bound <= state.cursor {
			return;
		}
		match self.queue.pull(state.cursor, bound, self.budget()) {
			CommitPull::Behind => self.request_load(state, ctx, bound),
			CommitPull::Hit {
				entries,
				advance_to,
				more,
			} => {
				let lsn = lsn_through(&entries, advance_to, state.lsn, frontier);
				if entries.is_empty() {
					self.moved(state, advance_to, lsn);
					if more {
						let _ = ctx.self_ref().send(SubscriptionReaderMessage::Wake);
					}
					return;
				}
				self.dispatch(state, ctx, entries, advance_to, lsn);
			}
		}
	}

	fn dispatch(
		&self,
		state: &mut SubscriptionReaderState,
		ctx: &Context<SubscriptionReaderMessage>,
		entries: Vec<Arc<CommitEntry>>,
		to: CommitVersion,
		lsn: Lsn,
	) {
		state.phase = Phase::Dispatching {
			to,
			lsn,
		};
		let reply = self.reply(state, ctx, |result| SubscriptionReaderMessage::Dispatched {
			result,
		});
		self.consumer.consume(entries, reply);
	}

	fn request_load(
		&self,
		state: &mut SubscriptionReaderState,
		ctx: &Context<SubscriptionReaderMessage>,
		bound: CommitVersion,
	) {
		state.phase = Phase::Loading;
		let reply = self.reply(state, ctx, |outcome| SubscriptionReaderMessage::Loaded {
			outcome,
		});
		let fetch = CommitLoaderMessage::Fetch {
			after: (state.cursor, state.lsn),
			up_to: bound,
			budget: self.load_budget(),
			reply,
		};
		if self.loader.send(fetch).is_err() && !ctx.is_cancelled() {
			self.abort_on_error(internal_error!(
				"subscription reader fell behind the commit queue at version {} but the commit loader is stopped",
				state.cursor.0
			));
		}
	}

	fn on_loaded(
		&self,
		state: &mut SubscriptionReaderState,
		ctx: &Context<SubscriptionReaderMessage>,
		outcome: Result<Loaded>,
	) {
		if !matches!(state.phase, Phase::Loading) {
			return;
		}
		state.phase = Phase::Ready;
		match outcome {
			Ok(loaded) => {
				let lsn = lsn_through(
					&loaded.entries,
					loaded.advance_to,
					state.lsn,
					(CommitVersion(0), Lsn::new(0)),
				);
				if loaded.entries.is_empty() {
					self.moved(state, loaded.advance_to, lsn);
					let _ = ctx.self_ref().send(SubscriptionReaderMessage::Wake);
					return;
				}
				self.dispatch(state, ctx, loaded.entries, loaded.advance_to, lsn);
			}
			Err(e) if TransactionError::is_commit_log_purged(&e) => self.terminate(state, ctx),
			Err(e) => self.abort_on_error(e),
		}
	}

	fn terminate(&self, state: &mut SubscriptionReaderState, ctx: &Context<SubscriptionReaderMessage>) {
		let (resume, lsn) = self.queue.released();
		state.phase = Phase::Terminating {
			resume,
			lsn: state.lsn.max(lsn),
		};
		let reply = self.reply(state, ctx, |result| SubscriptionReaderMessage::Terminated {
			result,
		});
		self.consumer.overtaken(state.cursor, resume, reply);
	}

	fn reply<T: Send + 'static>(
		&self,
		state: &mut SubscriptionReaderState,
		ctx: &Context<SubscriptionReaderMessage>,
		message: fn(Result<T>) -> SubscriptionReaderMessage,
	) -> Box<dyn FnOnce(Result<T>) + Send> {
		state.waiting_since = None;
		state.replied = Arc::new(AtomicBool::new(false));
		let replied = state.replied.clone();
		let self_ref = ctx.self_ref().clone();
		Box::new(move |result| {
			replied.store(true, Ordering::Release);
			let _ = self_ref.send(message(result));
		})
	}

	fn moved(&self, state: &mut SubscriptionReaderState, to: CommitVersion, lsn: Lsn) {
		state.cursor = to;
		state.lsn = lsn;
		self.floor.advance(to);
		self.watermark.store(to);
	}

	fn on_dispatched(
		&self,
		state: &mut SubscriptionReaderState,
		ctx: &Context<SubscriptionReaderMessage>,
		result: Result<()>,
	) {
		let Phase::Dispatching {
			to,
			lsn,
		} = state.phase
		else {
			return;
		};
		state.phase = Phase::Ready;
		match result {
			Ok(()) => {
				self.moved(state, to, lsn);
				let _ = ctx.self_ref().send(SubscriptionReaderMessage::Wake);
			}
			Err(e) if TransactionError::is_consumer_overtaken(&e)
				|| TransactionError::is_snapshot_evicted(&e) =>
			{
				self.terminate(state, ctx)
			}
			Err(e) => self.abort_on_error(e),
		}
	}

	fn on_terminated(
		&self,
		state: &mut SubscriptionReaderState,
		ctx: &Context<SubscriptionReaderMessage>,
		result: Result<()>,
	) {
		let Phase::Terminating {
			resume,
			lsn,
		} = state.phase
		else {
			return;
		};
		state.phase = Phase::Ready;
		match result {
			Ok(()) => {
				self.moved(state, resume, lsn);
				let _ = ctx.self_ref().send(SubscriptionReaderMessage::Wake);
			}
			Err(e) => self.abort_on_error(e),
		}
	}

	fn check_stall(&self, state: &mut SubscriptionReaderState) {
		let batch = match state.phase {
			Phase::Loading => "load".to_string(),
			Phase::Dispatching {
				to,
				..
			} => to.0.to_string(),
			Phase::Terminating {
				..
			} => "terminate".to_string(),
			Phase::Ready => return,
		};
		if state.replied.load(Ordering::Acquire) {
			return;
		}
		let Some(since) = &state.waiting_since else {
			state.waiting_since = Some(self.clock.instant());
			return;
		};
		if since.elapsed() < self.wait_timeout().to_std() {
			return;
		}
		fatal(FatalReport::new(
			FatalKind::Error,
			format!("consume reply not received within {:?}", self.wait_timeout()),
		)
		.component("subscription reader")
		.with("consumer", "subscription")
		.with("pending", self.consumer.describe_pending())
		.with("batch", batch)
		.backtrace(Backtrace::force_capture().to_string()))
	}

	fn abort_on_error(&self, err: Error) -> ! {
		fatal(FatalReport::new(FatalKind::Error, format!("{}", err))
			.component("subscription reader")
			.backtrace(Backtrace::force_capture().to_string()))
	}
}

impl Actor for SubscriptionReader {
	type State = SubscriptionReaderState;
	type Message = SubscriptionReaderMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		self.watermark.store(self.start);
		let _ = ctx.self_ref().send(SubscriptionReaderMessage::Wake);
		ctx.schedule_once(Duration::from_milliseconds(TICK_INTERVAL_MS).unwrap(), || {
			SubscriptionReaderMessage::Tick
		});
		SubscriptionReaderState {
			cursor: self.start,
			lsn: self.start_lsn,
			phase: Phase::Ready,
			waiting_since: None,
			replied: Arc::new(AtomicBool::new(false)),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			SubscriptionReaderMessage::Wake => self.advance(state, ctx),
			SubscriptionReaderMessage::Tick => {
				if ctx.is_cancelled() {
					return Directive::Stop;
				}
				self.check_stall(state);
				self.advance(state, ctx);
				ctx.schedule_once(Duration::from_milliseconds(TICK_INTERVAL_MS).unwrap(), || {
					SubscriptionReaderMessage::Tick
				});
			}
			SubscriptionReaderMessage::Loaded {
				outcome,
			} => self.on_loaded(state, ctx, outcome),
			SubscriptionReaderMessage::Dispatched {
				result,
			} => self.on_dispatched(state, ctx, result),
			SubscriptionReaderMessage::Terminated {
				result,
			} => self.on_terminated(state, ctx, result),
			SubscriptionReaderMessage::Shutdown => return Directive::Stop,
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}
