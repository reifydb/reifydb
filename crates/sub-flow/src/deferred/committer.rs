// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, VecDeque},
	panic::{AssertUnwindSafe, catch_unwind},
	process,
};

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::{catalog::flow::FlowId, cdc::CdcConsumerId, change::Change},
	key::{EncodableKey, Key, dictionary::DictionaryEntryKey, kind::KeyKind},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{
	Result,
	value::{dictionary::DictionaryId, identity::IdentityId},
};
use tracing::{error, instrument, warn};

use crate::{catalog::FlowCatalog, deferred::tracker::FlowPositionTracker};

pub type CommitterHandle = ActorHandle<CommitterMessage>;

pub enum CommitterMessage {
	Slice {
		slice: FlowSlice,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	},

	Tick {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		reply: Box<dyn FnOnce() + Send>,
	},

	Complete,
}

enum CommitJob {
	Slice {
		slice: FlowSlice,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	},
	Tick {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		reply: Box<dyn FnOnce() + Send>,
	},
}

pub struct CommitterActor {
	committer: Committer,
}

impl CommitterActor {
	pub fn new(committer: Committer) -> Self {
		Self {
			committer,
		}
	}

	fn maybe_dispatch(&self, state: &mut CommitterState, ctx: &Context<CommitterMessage>) {
		if state.committing {
			return;
		}
		let Some(job) = state.queue.pop_front() else {
			return;
		};
		state.committing = true;

		let committer = self.committer.clone();
		let self_ref = ctx.self_ref().clone();

		self.committer.engine.spawner().pools().commit_pool().spawn(move || {
			catch_unwind(AssertUnwindSafe(|| run_commit_job(&committer, job))).unwrap_or_else(|_| {
				error!("panic in flow committer, aborting");
				process::abort()
			});
			let _ = self_ref.send(CommitterMessage::Complete);
		});
	}
}

pub struct CommitterState {
	committing: bool,
	queue: VecDeque<CommitJob>,
}

impl Actor for CommitterActor {
	type State = CommitterState;
	type Message = CommitterMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		CommitterState {
			committing: false,
			queue: VecDeque::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CommitterMessage::Slice {
				slice,
				reply,
			} => {
				state.queue.push_back(CommitJob::Slice {
					slice,
					reply,
				});
				self.maybe_dispatch(state, ctx);
			}
			CommitterMessage::Tick {
				pending,
				pending_shapes,
				reply,
			} => {
				state.queue.push_back(CommitJob::Tick {
					pending,
					pending_shapes,
					reply,
				});
				self.maybe_dispatch(state, ctx);
			}
			CommitterMessage::Complete => {
				state.committing = false;
				self.maybe_dispatch(state, ctx);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

fn run_commit_job(committer: &Committer, job: CommitJob) {
	match job {
		CommitJob::Slice {
			slice,
			reply,
		} => {
			let result = committer.commit_slice(slice);
			(reply)(result);
		}
		CommitJob::Tick {
			pending,
			pending_shapes,
			reply,
		} => {
			committer.commit_tick(pending, pending_shapes);
			(reply)();
		}
	}
}

pub struct FlowSlice {
	pub combined: Pending,

	pub pending_shapes: Vec<RowShape>,

	pub checkpoints: Vec<(FlowId, CommitVersion)>,

	pub positions: Vec<(FlowId, CommitVersion)>,

	pub checkpoint_deletes: Vec<FlowId>,

	pub view_changes: Vec<Change>,

	pub control_cursor: Option<(CdcConsumerId, CommitVersion)>,
}

impl FlowSlice {
	pub fn empty() -> Self {
		Self {
			combined: Pending::new(),
			pending_shapes: Vec::new(),
			checkpoints: Vec::new(),
			positions: Vec::new(),
			checkpoint_deletes: Vec::new(),
			view_changes: Vec::new(),
			control_cursor: None,
		}
	}
}

#[derive(Clone)]
pub struct Committer {
	engine: StandardEngine,
	catalog: FlowCatalog,
	flow_tracker: FlowPositionTracker,
}

impl Committer {
	pub fn new(engine: StandardEngine, catalog: FlowCatalog, flow_tracker: FlowPositionTracker) -> Self {
		Self {
			engine,
			catalog,
			flow_tracker,
		}
	}

	#[instrument(name = "flow::committer::commit_slice", level = "debug", skip_all)]
	pub fn commit_slice(&self, slice: FlowSlice) -> Result<()> {
		let FlowSlice {
			combined,
			pending_shapes,
			checkpoints,
			positions,
			checkpoint_deletes,
			view_changes,
			control_cursor,
		} = slice;

		let mut transaction = self.engine.begin_command(IdentityId::system())?;
		transaction.disable_conflict_tracking()?;

		apply_pending_writes(&mut transaction, &combined)?;

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		for (flow_id, version) in &checkpoints {
			CdcCheckpoint::persist(&mut transaction, flow_id, *version)?;
		}

		for flow_id in &checkpoint_deletes {
			CdcCheckpoint::delete(&mut transaction, flow_id)?;
		}

		if let Some((consumer_id, version)) = &control_cursor {
			CdcCheckpoint::persist(&mut transaction, consumer_id, *version)?;
		}

		self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut transaction), pending_shapes)?;

		transaction.commit_unchecked()?;

		self.evict_durable_reservations(&combined);
		for (flow_id, version) in checkpoints.iter().chain(positions.iter()) {
			self.flow_tracker.update(*flow_id, *version);
		}

		for flow_id in &checkpoint_deletes {
			self.flow_tracker.remove(*flow_id);
		}
		Ok(())
	}

	#[instrument(name = "flow::committer::commit_tick", level = "debug", skip_all)]
	pub fn commit_tick(&self, pending: Pending, pending_shapes: Vec<RowShape>) {
		let mut transaction = match self.engine.begin_command(IdentityId::system()) {
			Ok(t) => t,
			Err(e) => {
				warn!(error = %e, "failed to begin command for tick commit");
				return;
			}
		};

		if let Err(e) = transaction.disable_conflict_tracking() {
			let _ = transaction.rollback();
			warn!(error = %e, "failed to disable conflict tracking for tick commit");
			return;
		}

		for (key, pw) in pending.iter_sorted() {
			let result = match pw {
				PendingWrite::Set(value) => transaction.set(key, value.clone()),
				PendingWrite::Remove => transaction.remove(key),
				PendingWrite::Drop => transaction.drop_key(key),
			};
			if let Err(e) = result {
				let _ = transaction.rollback();
				warn!(error = %e, "failed to apply tick write");
				return;
			}
		}

		if let Err(e) =
			self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut transaction), pending_shapes)
		{
			let _ = transaction.rollback();
			warn!(error = %e, "failed to persist tick pending shapes");
			return;
		}

		if let Err(e) = transaction.commit_unchecked() {
			warn!(error = %e, "failed to commit tick writes");
		} else {
			self.evict_durable_reservations(&pending);
		}
	}

	fn evict_durable_reservations(&self, committed: &Pending) {
		let registry = self.engine.dictionary_allocators();
		let mut by_dict: HashMap<DictionaryId, Vec<[u8; 16]>> = HashMap::new();
		for (key, _) in committed.iter_sorted() {
			if matches!(Key::kind(key), Some(KeyKind::DictionaryEntry))
				&& let Some(entry) = DictionaryEntryKey::decode(key)
			{
				by_dict.entry(entry.dictionary).or_default().push(entry.hash);
			}
		}
		for (dictionary, hashes) in by_dict {
			registry.mark_durable(dictionary, &hashes);
		}
	}
}

#[instrument(name = "flow::committer::apply_pending", level = "debug", skip_all)]
fn apply_pending_writes(transaction: &mut CommandTransaction, combined: &Pending) -> Result<()> {
	for (key, pw) in combined.iter_sorted() {
		match pw {
			PendingWrite::Set(value) => transaction.set(key, value.clone())?,
			PendingWrite::Remove => {
				if matches!(Key::kind(key), Some(KeyKind::Row)) {
					match transaction.get(key)? {
						Some(existing) => transaction.unset(key, existing.row)?,
						None => transaction.remove(key)?,
					}
				} else {
					transaction.remove(key)?;
				}
			}
			PendingWrite::Drop => transaction.drop_key(key)?,
		}
	}
	Ok(())
}
