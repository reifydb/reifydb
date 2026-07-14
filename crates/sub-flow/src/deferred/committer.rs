// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::{catalog::flow::FlowId, cdc::CdcConsumerId, change::Change},
	key::{Key, kind::KeyKind},
};
#[cfg(test)]
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_transaction::{
	group::{GroupCommitApply, GroupCommitCompletion, GroupCommitHandle, GroupCommitSubmission},
	transaction::{Transaction, command::CommandTransaction},
};
use reifydb_value::Result;
#[cfg(test)]
use reifydb_value::value::identity::IdentityId;
use tracing::{instrument, warn};

use crate::{catalog::FlowCatalog, deferred::tracker::FlowPositionTracker};

pub type CommitterHandle = ActorHandle<CommitterMessage>;

pub(crate) type SliceCommitReply = Box<dyn FnOnce(Result<(CommitVersion, Pending)>) + Send>;
pub(crate) type TickCommitReply = Box<dyn FnOnce(Option<(CommitVersion, Pending)>) + Send>;

pub enum CommitterMessage {
	Slice {
		slice: FlowSlice,
		reply: SliceCommitReply,
	},

	Tick {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		reply: TickCommitReply,
	},
}

pub struct CommitterActor {
	committer: Committer,
	group: GroupCommitHandle,
}

impl CommitterActor {
	pub fn new(committer: Committer, group: GroupCommitHandle) -> Self {
		Self {
			committer,
			group,
		}
	}

	fn submit_slice(&self, slice: FlowSlice, reply: SliceCommitReply) {
		let FlowSlice {
			combined,
			pending_shapes,
			checkpoints,
			positions,
			checkpoint_deletes,
			view_changes,
			control_cursor,
		} = slice;
		let combined = Arc::new(combined);

		let apply_committer = self.committer.clone();
		let apply_combined = Arc::clone(&combined);
		let apply_checkpoints = checkpoints.clone();
		let apply_deletes = checkpoint_deletes.clone();
		let apply: GroupCommitApply = Box::new(move |transaction| {
			apply_committer.apply_slice(
				transaction,
				&apply_combined,
				pending_shapes,
				&apply_checkpoints,
				&apply_deletes,
				view_changes,
				&control_cursor,
			)
		});

		let completion_committer = self.committer.clone();
		let completion: GroupCommitCompletion = Box::new(move |result| match result {
			Ok(version) => {
				completion_committer.post_commit_slice(&checkpoints, &positions, &checkpoint_deletes);
				let combined = Arc::try_unwrap(combined).unwrap_or_else(|shared| (*shared).clone());
				(reply)(Ok((version, combined)));
			}
			Err(e) => (reply)(Err(e)),
		});

		self.group.submit(GroupCommitSubmission {
			apply,
			completion,
		});
	}

	fn submit_tick(&self, pending: Pending, pending_shapes: Vec<RowShape>, reply: TickCommitReply) {
		let pending = Arc::new(pending);

		let apply_committer = self.committer.clone();
		let apply_pending = Arc::clone(&pending);
		let apply: GroupCommitApply = Box::new(move |transaction| {
			apply_committer.apply_tick(transaction, &apply_pending, pending_shapes)
		});

		let _completion_committer = self.committer.clone();
		let completion: GroupCommitCompletion = Box::new(move |result| match result {
			Ok(version) => {
				let pending = Arc::try_unwrap(pending).unwrap_or_else(|shared| (*shared).clone());
				(reply)(Some((version, pending)));
			}
			Err(e) => {
				warn!(error = %e, "failed to commit tick writes");
				(reply)(None);
			}
		});

		self.group.submit(GroupCommitSubmission {
			apply,
			completion,
		});
	}
}

impl Actor for CommitterActor {
	type State = ();
	type Message = CommitterMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CommitterMessage::Slice {
				slice,
				reply,
			} => self.submit_slice(slice, reply),
			CommitterMessage::Tick {
				pending,
				pending_shapes,
				reply,
			} => self.submit_tick(pending, pending_shapes, reply),
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
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
	catalog: FlowCatalog,
	flow_tracker: FlowPositionTracker,
}

impl Committer {
	pub fn new(catalog: FlowCatalog, flow_tracker: FlowPositionTracker) -> Self {
		Self {
			catalog,
			flow_tracker,
		}
	}

	#[instrument(name = "flow::committer::apply_slice", level = "debug", skip_all)]
	#[allow(clippy::too_many_arguments)]
	fn apply_slice(
		&self,
		transaction: &mut CommandTransaction,
		combined: &Pending,
		pending_shapes: Vec<RowShape>,
		checkpoints: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
		view_changes: Vec<Change>,
		control_cursor: &Option<(CdcConsumerId, CommitVersion)>,
	) -> Result<()> {
		apply_pending_writes(transaction, combined)?;

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		for (flow_id, version) in checkpoints {
			CdcCheckpoint::persist(transaction, flow_id, *version)?;
		}

		for flow_id in checkpoint_deletes {
			CdcCheckpoint::delete(transaction, flow_id)?;
		}

		if let Some((consumer_id, version)) = control_cursor {
			CdcCheckpoint::persist(transaction, consumer_id, *version)?;
		}

		self.catalog.persist_pending_shapes(&mut Transaction::Command(transaction), pending_shapes)
	}

	fn post_commit_slice(
		&self,
		checkpoints: &[(FlowId, CommitVersion)],
		positions: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
	) {
		for (flow_id, version) in checkpoints.iter().chain(positions.iter()) {
			self.flow_tracker.update(*flow_id, *version);
		}

		for flow_id in checkpoint_deletes {
			self.flow_tracker.remove(*flow_id);
		}
	}

	#[instrument(name = "flow::committer::apply_tick", level = "debug", skip_all)]
	fn apply_tick(
		&self,
		transaction: &mut CommandTransaction,
		pending: &Pending,
		pending_shapes: Vec<RowShape>,
	) -> Result<()> {
		for (key, pw) in pending.iter_sorted() {
			match pw {
				PendingWrite::Set(value) => transaction.set(key, value.clone())?,
				PendingWrite::Remove => transaction.remove(key)?,
				PendingWrite::Drop => transaction.drop_key(key)?,
			}
		}

		self.catalog.persist_pending_shapes(&mut Transaction::Command(transaction), pending_shapes)
	}
}

#[cfg(test)]
impl Committer {
	#[instrument(name = "flow::committer::commit_slice", level = "debug", skip_all)]
	pub fn commit_slice(&self, engine: &StandardEngine, slice: FlowSlice) -> Result<(CommitVersion, Pending)> {
		let FlowSlice {
			combined,
			pending_shapes,
			checkpoints,
			positions,
			checkpoint_deletes,
			view_changes,
			control_cursor,
		} = slice;

		let mut transaction = engine.begin_command(IdentityId::system())?;
		transaction.disable_conflict_tracking()?;

		self.apply_slice(
			&mut transaction,
			&combined,
			pending_shapes,
			&checkpoints,
			&checkpoint_deletes,
			view_changes,
			&control_cursor,
		)?;

		let commit_version = transaction.commit_unchecked()?;

		self.post_commit_slice(&checkpoints, &positions, &checkpoint_deletes);
		Ok((commit_version, combined))
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

#[cfg(test)]
mod group_commit_integration {
	use std::{
		sync::atomic::{AtomicUsize, Ordering},
		thread::sleep,
		time::Duration as StdDuration,
	};

	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_core::interface::cdc::SystemChange;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::sync::{mutex::Mutex, waiter::WaiterHandle};
	use reifydb_transaction::group::GroupCommitBegin;
	use reifydb_value::{util::cowvec::CowVec, value::duration::Duration};

	use super::*;

	struct SliceReplies {
		results: Mutex<Vec<(usize, Result<(CommitVersion, Pending)>)>>,
		remaining: AtomicUsize,
		done: WaiterHandle,
	}

	impl SliceReplies {
		fn new(expected: usize) -> Arc<Self> {
			Arc::new(Self {
				results: Mutex::new(Vec::new()),
				remaining: AtomicUsize::new(expected),
				done: WaiterHandle::new(),
			})
		}

		fn reply(self: &Arc<Self>, index: usize) -> SliceCommitReply {
			let replies = Arc::clone(self);
			Box::new(move |result| {
				replies.results.lock().push((index, result));
				if replies.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
					replies.done.notify();
				}
			})
		}

		fn wait(&self) {
			assert!(self.done.wait_timeout(Duration::from_seconds(10).unwrap()), "slice replies timed out");
		}

		fn versions(&self) -> Vec<(usize, CommitVersion)> {
			self.results
				.lock()
				.iter()
				.map(|(i, r)| (*i, r.as_ref().expect("expected committed slice").0))
				.collect()
		}
	}

	fn synthetic_key(index: u64) -> EncodedKey {
		// 0xEE maps to no KeyKind: every CDC consumer ignores it, but the producer
		// includes unknown kinds, so the write is observable in the CDC record.
		EncodedKey::new(vec![0xEE, index as u8])
	}

	fn synthetic_slice(index: u64) -> FlowSlice {
		let mut combined = Pending::new();
		combined.insert(synthetic_key(index), EncodedRow(CowVec::new(vec![index as u8; 4])));
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(index), CommitVersion(100 + index))];
		slice
	}

	fn build_committer_actor(engine: &StandardEngine, group: GroupCommitHandle) -> (CommitterHandle, Committer) {
		let tracker = FlowPositionTracker::new();
		let committer = Committer::new(FlowCatalog::new(engine.catalog()), tracker.clone());
		let handle = engine
			.spawner()
			.spawn_flow("group-commit-test-committer", CommitterActor::new(committer.clone(), group));
		(handle, committer)
	}

	fn send_slices(handle: &CommitterHandle, replies: &Arc<SliceReplies>, count: usize) {
		for i in 0..count {
			let sent = handle
				.actor_ref()
				.send(CommitterMessage::Slice {
					slice: synthetic_slice(i as u64 + 1),
					reply: replies.reply(i),
				})
				.is_ok();
			assert!(sent, "send slice");
		}
	}

	#[test]
	fn grouped_slices_share_one_version_and_one_cdc_record() {
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let group = GroupCommitHandle::spawn(
			&engine.spawner(),
			begin,
			Duration::from_milliseconds(50).unwrap(),
			16,
		);
		let (handle, committer) = build_committer_actor(&engine, group);

		let replies = SliceReplies::new(3);
		send_slices(&handle, &replies, 3);
		replies.wait();

		let versions = replies.versions();
		assert_eq!(versions.len(), 3);
		let shared = versions[0].1;
		assert!(shared > CommitVersion(0));
		assert!(
			versions.iter().all(|(_, v)| *v == shared),
			"all flows' slices must share one commit version: {versions:?}"
		);

		let tracked = committer.flow_tracker.all();
		for i in 1..=3u64 {
			assert_eq!(
				tracked.get(&FlowId(i)).copied(),
				Some(CommitVersion(100 + i)),
				"tracker must be updated per flow"
			);
		}

		// CDC production is async (event bus -> producer actor); poll until the
		// record for the shared version lands.
		let cdc_store = engine.cdc_store();
		let mut record = None;
		for _ in 0..400 {
			if let Some(cdc) = cdc_store.read(shared).expect("cdc read") {
				record = Some(cdc);
				break;
			}
			sleep(StdDuration::from_millis(5));
		}
		let record = record.expect("one CDC record must exist at the shared version");

		let expected: Vec<EncodedKey> = (1..=3).map(synthetic_key).collect();
		let written: Vec<EncodedKey> = record
			.system_changes
			.iter()
			.filter_map(|change| match change {
				SystemChange::Insert {
					key,
					..
				} => expected.contains(key).then(|| key.clone()),
				_ => None,
			})
			.collect();
		assert_eq!(
			written, expected,
			"the merged CDC record must contain every slice's writes in submission order"
		);
	}

	#[test]
	fn inline_handle_commits_each_slice_in_its_own_version() {
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let group = GroupCommitHandle::inline(begin);
		let (handle, committer) = build_committer_actor(&engine, group);

		let replies = SliceReplies::new(2);
		send_slices(&handle, &replies, 2);
		replies.wait();

		let mut versions: Vec<CommitVersion> = replies.versions().iter().map(|(_, v)| *v).collect();
		versions.sort();
		assert_eq!(versions.len(), 2);
		assert!(
			versions[0] < versions[1],
			"passthrough mode must commit each slice in its own version: {versions:?}"
		);

		let tracked = committer.flow_tracker.all();
		for i in 1..=2u64 {
			assert_eq!(tracked.get(&FlowId(i)).copied(), Some(CommitVersion(100 + i)));
		}
	}
}
