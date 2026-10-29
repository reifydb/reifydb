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
		queue::{CommitPull, CommitQueue, CommitQueueFloor},
		watermark::CdcConsumerWatermark,
	},
	common::CommitVersion,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		cdc::CdcConsumerId,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
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

use crate::consumer::{Reply, SubscriptionCdcConsumer};

const TICK_INTERVAL_MS: i64 = 10;

pub enum SubscriptionReaderMessage {
	Wake,
	Tick,
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
	Dispatching {
		to: CommitVersion,
	},
	Terminating {
		resume: CommitVersion,
	},
}

pub struct SubscriptionReaderState {
	cursor: CommitVersion,
	phase: Phase,
	waiting_since: Option<Instant>,
	replied: Arc<AtomicBool>,
}

pub struct SubscriptionReader {
	engine: StandardEngine,
	queue: CommitQueue,
	floor: CommitQueueFloor,
	consumer: SubscriptionCdcConsumer,
	watermark: CdcConsumerWatermark,
	clock: Clock,
	start: CommitVersion,
}

impl SubscriptionReader {
	pub fn new(
		engine: StandardEngine,
		queue: CommitQueue,
		floor: CommitQueueFloor,
		consumer: SubscriptionCdcConsumer,
		watermark: CdcConsumerWatermark,
		start: CommitVersion,
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
		}
	}

	fn budget(&self) -> ByteSize {
		ByteSize::from_bytes(self.engine.catalog().get_config_uint8(ConfigKey::FlowPullBatchBytes))
	}

	fn wait_timeout(&self) -> Duration {
		self.engine.catalog().get_config_duration(ConfigKey::CdcConsumeWaitTimeout)
	}

	fn advance(&self, state: &mut SubscriptionReaderState, ctx: &Context<SubscriptionReaderMessage>) {
		if !matches!(state.phase, Phase::Ready) {
			return;
		}
		self.floor.disarm();
		let released = self.queue.released_through();
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
			CommitPull::Behind => self.terminate(state, ctx),
			CommitPull::Hit {
				entries,
				advance_to,
				more,
			} => {
				if entries.is_empty() {
					self.moved(state, advance_to);
					if more {
						let _ = ctx.self_ref().send(SubscriptionReaderMessage::Wake);
					}
					return;
				}
				state.phase = Phase::Dispatching {
					to: advance_to,
				};
				let reply = self.reply(state, ctx, |result| SubscriptionReaderMessage::Dispatched {
					result,
				});
				self.consumer.consume(entries, reply);
			}
		}
	}

	fn terminate(&self, state: &mut SubscriptionReaderState, ctx: &Context<SubscriptionReaderMessage>) {
		let resume = self.queue.released().0;
		state.phase = Phase::Terminating {
			resume,
		};
		let reply = self.reply(state, ctx, |result| SubscriptionReaderMessage::Terminated {
			result,
		});
		self.consumer.overtaken(state.cursor, resume, reply);
	}

	fn reply(
		&self,
		state: &mut SubscriptionReaderState,
		ctx: &Context<SubscriptionReaderMessage>,
		message: fn(Result<()>) -> SubscriptionReaderMessage,
	) -> Reply {
		state.waiting_since = None;
		state.replied = Arc::new(AtomicBool::new(false));
		let replied = state.replied.clone();
		let self_ref = ctx.self_ref().clone();
		Box::new(move |result| {
			replied.store(true, Ordering::Release);
			let _ = self_ref.send(message(result));
		})
	}

	fn moved(&self, state: &mut SubscriptionReaderState, to: CommitVersion) {
		state.cursor = to;
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
		} = state.phase
		else {
			return;
		};
		state.phase = Phase::Ready;
		match result {
			Ok(()) => {
				self.moved(state, to);
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
		} = state.phase
		else {
			return;
		};
		state.phase = Phase::Ready;
		match result {
			Ok(()) => {
				self.moved(state, resume);
				let _ = ctx.self_ref().send(SubscriptionReaderMessage::Wake);
			}
			Err(e) => self.abort_on_error(e),
		}
	}

	fn check_stall(&self, state: &mut SubscriptionReaderState) {
		let batch = match state.phase {
			Phase::Dispatching {
				to,
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
		.with("consumer", format!("{:?}", CdcConsumerId::subscription_consumer()))
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
