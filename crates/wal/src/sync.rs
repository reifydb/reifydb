// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	timers::TimerHandle,
	traits::{Actor, Directive},
};
use reifydb_value::value::duration::Duration;

use crate::{
	body::Body,
	device::{Append, Flush},
	wal::Wal,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
	GroupCommit {
		window: Duration,
	},
	EveryAppend,
	Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMessage {
	Tick,
	Appended,
}

pub(crate) struct SyncActor<T, L> {
	wal: Wal<T, L>,
	window: Option<Duration>,
}

pub(crate) struct SyncState {
	_timer: Option<TimerHandle>,
}

impl<T, L> SyncActor<T, L> {
	pub(crate) fn new(wal: Wal<T, L>, window: Option<Duration>) -> Self {
		Self {
			wal,
			window,
		}
	}
}

impl<T: Body + Send + Sync + 'static, L: Append + Flush> Actor for SyncActor<T, L> {
	type State = SyncState;

	type Message = SyncMessage;

	fn init(&self, ctx: &Context<SyncMessage>) -> SyncState {
		SyncState {
			_timer: self.window.map(|window| ctx.schedule_tick(window, |_| SyncMessage::Tick)),
		}
	}

	fn handle(&self, _state: &mut SyncState, _message: SyncMessage, ctx: &Context<SyncMessage>) -> Directive {
		if let Err(error) = self.wal.sync() {
			panic!("the wal sync actor could not flush: {error}");
		}
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(1)
	}
}
