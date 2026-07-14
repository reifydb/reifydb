// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	mem,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
};

use reifydb_core::{common::CommitVersion, internal_error};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		traits::{Actor, Directive},
	},
	sync::waiter::WaiterHandle,
};
use reifydb_value::{Result, error::Error, value::duration::Duration};
use tracing::{error, instrument};

use crate::transaction::command::CommandTransaction;

pub type GroupCommitApply = Box<dyn FnOnce(&mut CommandTransaction) -> Result<()> + Send>;

pub type GroupCommitCompletion = Box<dyn FnOnce(Result<CommitVersion>) + Send>;

pub struct GroupCommitSubmission {
	pub apply: GroupCommitApply,
	pub completion: GroupCommitCompletion,
}

pub type GroupCommitBegin = Arc<dyn Fn() -> Result<CommandTransaction> + Send + Sync>;

pub enum GroupCommitMessage {
	Submit(GroupCommitSubmission),
	Flush {
		generation: u64,
	},
	Shutdown {
		waiter: Arc<WaiterHandle>,
	},
}

pub struct GroupCommitActor {
	begin: GroupCommitBegin,
	linger: Duration,
	max_entries: usize,
}

pub struct GroupCommitState {
	pending: Vec<GroupCommitSubmission>,
	generation: u64,
	draining: bool,
}

impl GroupCommitActor {
	fn flush(&self, state: &mut GroupCommitState) {
		state.generation += 1;
		let submissions = mem::take(&mut state.pending);
		commit_group(&self.begin, submissions);
	}
}

impl Actor for GroupCommitActor {
	type State = GroupCommitState;
	type Message = GroupCommitMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		GroupCommitState {
			pending: Vec::new(),
			generation: 0,
			draining: false,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			GroupCommitMessage::Submit(submission) => {
				if state.draining {
					(submission.completion)(Err(internal_error!(
						"group commit coordinator is shut down"
					)));
					return Directive::Continue;
				}
				state.pending.push(submission);
				if state.pending.len() >= self.max_entries {
					self.flush(state);
				} else if state.pending.len() == 1 {
					let generation = state.generation;
					let _ = ctx.schedule_once(self.linger, move || GroupCommitMessage::Flush {
						generation,
					});
				}
			}
			GroupCommitMessage::Flush {
				generation,
			} => {
				if generation == state.generation && !state.pending.is_empty() {
					self.flush(state);
				}
			}
			GroupCommitMessage::Shutdown {
				waiter,
			} => {
				if !state.pending.is_empty() {
					self.flush(state);
				}
				state.draining = true;
				waiter.notify();
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

#[derive(Clone)]
enum HandleInner {
	Inline(GroupCommitBegin),
	Grouped(ActorRef<GroupCommitMessage>),
}

#[derive(Clone)]
pub struct GroupCommitHandle {
	inner: HandleInner,
}

impl GroupCommitHandle {
	pub fn inline(begin: GroupCommitBegin) -> Self {
		Self {
			inner: HandleInner::Inline(begin),
		}
	}

	#[cfg(not(reifydb_single_threaded))]
	pub fn spawn(spawner: &ActorSpawner, begin: GroupCommitBegin, linger: Duration, max_entries: usize) -> Self {
		let actor = GroupCommitActor {
			begin,
			linger,
			max_entries: max_entries.max(1),
		};
		let actor_ref = spawner.spawn_coordination("group-commit", actor).actor_ref().clone();
		Self {
			inner: HandleInner::Grouped(actor_ref),
		}
	}

	#[cfg(reifydb_single_threaded)]
	pub fn spawn(_spawner: &ActorSpawner, begin: GroupCommitBegin, _linger: Duration, _max_entries: usize) -> Self {
		Self::inline(begin)
	}

	#[instrument(name = "transaction::group::submit", level = "debug", skip_all)]
	pub fn submit(&self, submission: GroupCommitSubmission) {
		match &self.inner {
			HandleInner::Inline(begin) => commit_group(begin, vec![submission]),
			HandleInner::Grouped(actor_ref) => {
				if let Err(send_error) = actor_ref.send(GroupCommitMessage::Submit(submission)) {
					let GroupCommitMessage::Submit(submission) = send_error.into_inner() else {
						return;
					};
					(submission.completion)(Err(internal_error!(
						"group commit coordinator unavailable"
					)));
				}
			}
		}
	}

	pub fn shutdown(&self) {
		let HandleInner::Grouped(actor_ref) = &self.inner else {
			return;
		};
		let waiter = Arc::new(WaiterHandle::new());
		let waiter_for_msg = Arc::clone(&waiter);
		if actor_ref
			.send_blocking(GroupCommitMessage::Shutdown {
				waiter: waiter_for_msg,
			})
			.is_err()
		{
			return;
		}
		waiter.wait_timeout(Duration::from_seconds(60).unwrap());
	}
}

fn commit_group(begin: &GroupCommitBegin, submissions: Vec<GroupCommitSubmission>) {
	catch_unwind(AssertUnwindSafe(|| run_group(begin, submissions))).unwrap_or_else(|_| {
		error!("panic in group commit, aborting");
		process::abort()
	});
}

#[instrument(name = "transaction::group::flush", level = "debug", skip_all)]
fn run_group(begin: &GroupCommitBegin, submissions: Vec<GroupCommitSubmission>) {
	let mut applies = Vec::with_capacity(submissions.len());
	let mut completions = Vec::with_capacity(submissions.len());
	for submission in submissions {
		applies.push(submission.apply);
		completions.push(submission.completion);
	}

	match apply_and_commit(begin, applies) {
		Ok(version) => {
			for completion in completions {
				(completion)(Ok(version));
			}
		}
		Err(e) => {
			for completion in completions {
				(completion)(Err(Error(e.0.clone())));
			}
		}
	}
}

fn apply_and_commit(begin: &GroupCommitBegin, applies: Vec<GroupCommitApply>) -> Result<CommitVersion> {
	let mut transaction = (begin)()?;
	if let Err(e) = transaction.disable_conflict_tracking() {
		let _ = transaction.rollback();
		return Err(e);
	}
	for apply in applies {
		if let Err(e) = (apply)(&mut transaction) {
			let _ = transaction.rollback();
			return Err(e);
		}
	}
	transaction.commit_unchecked()
}
