// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	delta::RemoveVisibility,
	interface::{
		catalog::flow::FlowId,
		cdc::{CdcConsumerId, ConsumerClass},
		change::Change,
	},
	key::{Key, kind::KeyKind},
};
#[cfg(test)]
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::transaction::substrate::{apply_operator_state, apply_operator_state_with_checkpoints};
use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	group::{GroupCommitApply, GroupCommitCompletion, GroupCommitHandle, GroupCommitSubmission},
	transaction::command::CommandTransaction,
};
use reifydb_value::Result;
#[cfg(test)]
use reifydb_value::value::identity::IdentityId;
use tracing::{instrument, warn};

use crate::{commit::quiescence::FlowMaterialization, progress::tracker::FlowPositionTracker};

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
		view_changes: Vec<Change>,
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
			checkpoints,
			checkpoint_deletes,
			view_changes,
			control_cursor,
		} = slice;
		let produced_output = combined.iter_sorted().next().is_some() || !view_changes.is_empty();
		let combined = Arc::new(combined);

		let apply_committer = self.committer.clone();
		let apply_combined = Arc::clone(&combined);
		let apply: GroupCommitApply = Box::new(move |transaction| {
			apply_committer.apply_slice(transaction, &apply_combined, view_changes, &control_cursor)
		});

		let completion_committer = self.committer.clone();
		let completion: GroupCommitCompletion = Box::new(move |result| match result {
			Ok(version) => {
				apply_operator_state_with_checkpoints(
					&completion_committer.operators,
					&combined,
					&checkpoints,
					&checkpoint_deletes,
				);
				if produced_output {
					completion_committer.materialization.record_output(version);
				}
				completion_committer.post_commit_slice(&checkpoints, &checkpoint_deletes);
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

	fn submit_tick(&self, pending: Pending, view_changes: Vec<Change>, reply: TickCommitReply) {
		let pending = Arc::new(pending);

		let apply_committer = self.committer.clone();
		let apply_pending = Arc::clone(&pending);
		let apply: GroupCommitApply = Box::new(move |transaction| {
			apply_committer.apply_tick(transaction, &apply_pending, view_changes)
		});

		let completion_committer = self.committer.clone();
		let completion: GroupCommitCompletion = Box::new(move |result| match result {
			Ok(version) => {
				apply_operator_state(&completion_committer.operators, &pending);
				completion_committer.materialization.record_output(version);
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
				view_changes,
				reply,
			} => self.submit_tick(pending, view_changes, reply),
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

pub struct FlowSlice {
	pub combined: Pending,

	pub checkpoints: Vec<(FlowId, CommitVersion)>,

	pub checkpoint_deletes: Vec<FlowId>,

	pub view_changes: Vec<Change>,

	pub control_cursor: Option<(CdcConsumerId, CommitVersion)>,
}

impl FlowSlice {
	pub fn empty() -> Self {
		Self {
			combined: Pending::new(),
			checkpoints: Vec::new(),
			checkpoint_deletes: Vec::new(),
			view_changes: Vec::new(),
			control_cursor: None,
		}
	}
}

#[derive(Clone)]
pub struct Committer {
	flow_tracker: FlowPositionTracker,
	materialization: FlowMaterialization,
	operators: OperatorStore,
}

impl Committer {
	pub fn new(
		flow_tracker: FlowPositionTracker,
		materialization: FlowMaterialization,
		operators: OperatorStore,
	) -> Self {
		Self {
			flow_tracker,
			materialization,
			operators,
		}
	}

	#[instrument(name = "flow::committer::apply_slice", level = "debug", skip_all)]
	fn apply_slice(
		&self,
		transaction: &mut CommandTransaction,
		combined: &Pending,
		view_changes: Vec<Change>,
		control_cursor: &Option<(CdcConsumerId, CommitVersion)>,
	) -> Result<()> {
		apply_pending_writes(transaction, combined)?;

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		if let Some((consumer_id, version)) = control_cursor {
			CdcCheckpoint::persist(transaction, consumer_id, *version, ConsumerClass::Pinning)?;
		}

		Ok(())
	}

	fn post_commit_slice(&self, checkpoints: &[(FlowId, CommitVersion)], checkpoint_deletes: &[FlowId]) {
		for (flow_id, version) in checkpoints {
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
		view_changes: Vec<Change>,
	) -> Result<()> {
		apply_pending_writes(transaction, pending)?;

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		Ok(())
	}
}

#[cfg(test)]
impl Committer {
	#[instrument(name = "flow::committer::commit_slice", level = "debug", skip_all)]
	pub fn commit_slice(&self, engine: &StandardEngine, slice: FlowSlice) -> Result<(CommitVersion, Pending)> {
		let FlowSlice {
			combined,
			checkpoints,
			checkpoint_deletes,
			view_changes,
			control_cursor,
		} = slice;

		let mut transaction = engine.begin_command(IdentityId::system())?;
		transaction.disable_conflict_tracking()?;

		self.apply_slice(&mut transaction, &combined, view_changes, &control_cursor)?;

		let commit_version = transaction.commit_unchecked()?;

		apply_operator_state_with_checkpoints(&self.operators, &combined, &checkpoints, &checkpoint_deletes);
		self.post_commit_slice(&checkpoints, &checkpoint_deletes);
		Ok((commit_version, combined))
	}
}

#[instrument(name = "flow::committer::apply_pending", level = "debug", skip_all)]
fn apply_pending_writes(transaction: &mut CommandTransaction, combined: &Pending) -> Result<()> {
	for (key, pw) in combined.iter_sorted() {
		if matches!(Key::kind(key), Some(KeyKind::OperatorState)) {
			continue;
		}
		match pw {
			PendingWrite::Set(value) => transaction.set(key, value.clone())?,
			PendingWrite::Remove {
				announce: RemoveVisibility::Announced,
			} => {
				if matches!(Key::kind(key), Some(KeyKind::Row | KeyKind::SeriesRow)) {
					match transaction.get(key)? {
						Some(existing) => transaction.remove_with_pre(key, existing.bytes)?,
						None => transaction.remove(key)?,
					}
				} else {
					transaction.remove(key)?;
				}
			}
			PendingWrite::Remove {
				announce: RemoveVisibility::Unobserved,
			} => {
				if matches!(Key::kind(key), Some(KeyKind::Row | KeyKind::SeriesRow)) {
					match transaction.get(key)? {
						Some(existing) => {
							transaction.remove_unobserved_with_pre(key, existing.bytes)?
						}
						None => transaction.remove_unobserved(key)?,
					}
				} else {
					transaction.remove_unobserved(key)?;
				}
			}
			PendingWrite::Remove {
				announce: RemoveVisibility::Silent,
			} => transaction.remove_silent(key)?,
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

	use reifydb_cdc::consume::watermark::CdcConsumerWatermark;
	use reifydb_codec::{
		key::encoded::EncodedKey,
		row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
	};
	use reifydb_core::{
		interface::{catalog::flow::OperatorId, cdc::CdcChange},
		internal_error,
		key::{
			EncodableKey,
			cdc_consumer::{CdcConsumerKey, CdcConsumerKeyRange},
			operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
		},
	};
	use reifydb_runtime::sync::{mutex::Mutex, waiter::WaiterHandle};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{group::GroupCommitBegin, multi::RangeScope, transaction::Transaction};
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::duration::Duration};

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
		combined.insert(synthetic_key(index), EncodedBytes(CowVec::new(vec![index as u8; 4])));
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(index), CommitVersion(100 + index))];
		slice
	}

	fn build_committer_actor(engine: &StandardEngine, group: GroupCommitHandle) -> (CommitterHandle, Committer) {
		let tracker = FlowPositionTracker::new();
		let committer = Committer::new(
			tracker.clone(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
		);
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

		// CDC production is async, so the record for the shared version has to be polled for.
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
			.changes
			.iter()
			.filter_map(|change| match change {
				CdcChange::Insert {
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

	#[test]
	fn a_committed_slice_writes_its_checkpoint_to_the_operator_store_and_no_consumer_row_to_the_multi_store() {
		// the checkpoint has one home now, and a second copy in the multi store would drift from the state it
		// is supposed to pair with
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let group = GroupCommitHandle::inline(begin);
		let (handle, committer) = build_committer_actor(&engine, group);

		let replies = SliceReplies::new(2);
		send_slices(&handle, &replies, 2);
		replies.wait();

		let mut query = engine.begin_query(IdentityId::system()).expect("begin query");
		let mut consumers: Vec<String> = Vec::new();
		for multi in Transaction::Query(&mut query)
			.range(CdcConsumerKeyRange::full_scan(), RangeScope::All, 1024)
			.expect("scan consumer checkpoints")
		{
			let multi = multi.expect("consumer checkpoint row");
			if let Some(key) = CdcConsumerKey::decode(&multi.key) {
				consumers.push(key.consumer.as_ref().to_string());
			}
		}

		assert!(
			!consumers.iter().any(|consumer| consumer.starts_with("flow:")),
			"a committed slice must leave no per-flow consumer checkpoint in the multi store; a row here \
			 is a second copy of the checkpoint that no longer moves with the operator state, so a crash \
			 can leave it ahead of that state: {consumers:?}"
		);

		for flow in 1..=2u64 {
			assert_eq!(
				committer.operators.checkpoint_get(FlowId(flow)),
				Some(CommitVersion(100 + flow)),
				"the checkpoint must instead land in the operator store, where the flush writes it in \
				 the same transaction as the state it belongs to"
			);
		}
	}

	fn state_inner(suffix: &[u8]) -> GroupStateKey {
		OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::CUSTOM, suffix)
	}

	fn state_slice(entries: &[(OperatorId, &GroupStateKey, u8)]) -> FlowSlice {
		let mut combined = Pending::new();
		for (operator, inner, tag) in entries {
			let (group, keyspace, suffix) = OperatorStateKey::decode_inner(inner.as_slice())
				.expect("test fixture group-state key must decode");
			combined.insert(
				OperatorStateKey::encoded(*operator, group, keyspace, suffix),
				EncodedOperatorRow::timeless(&[*tag; 4]).into_bytes(),
			);
		}
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(1), CommitVersion(10))];
		slice
	}

	#[test]
	fn a_failed_group_commit_leaves_the_operator_state_untouched() {
		// A rolled-back group must leave no operator state: otherwise flows read versions that
		// never became durable. Falsified by applying operator state writes on the failure side or
		// inside the apply closure.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		// max_entries = 2 flushes when the poison joins the slice; slice first so its apply
		// runs before the sibling fails the group.
		let group = GroupCommitHandle::spawn(&engine.spawner(), begin, Duration::from_seconds(5).unwrap(), 2);
		let (handle, committer) = build_committer_actor(&engine, group.clone());
		let store = committer.operators.clone();

		let operator = OperatorId(9);
		let inner = state_inner(b"k");
		let slice = state_slice(&[(operator, &inner, 7)]);

		let replies = SliceReplies::new(1);
		assert!(handle
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply: replies.reply(0),
			})
			.is_ok());
		sleep(StdDuration::from_millis(200));

		group.submit(GroupCommitSubmission {
			apply: Box::new(|_| Err(internal_error!("poisoned sibling"))),
			completion: Box::new(|_| {}),
		});
		replies.wait();

		{
			let results = replies.results.lock();
			assert!(results[0].1.is_err(), "the poisoned group must fail the slice commit");
		}
		assert_eq!(
			store.get(operator, &EncodedKey::new(inner.as_slice())),
			None,
			"a failed commit must not leak its operator-state writes into the store"
		);
		assert_eq!(
			store.total_bytes(),
			ByteSize::ZERO,
			"the operator state store must be byte-for-byte untouched"
		);
	}

	#[test]
	fn operator_state_becomes_visible_only_with_the_commit() {
		// Operator state must never appear before the commit completes, falsified by applying at submission.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let group = GroupCommitHandle::spawn(
			&engine.spawner(),
			begin,
			Duration::from_milliseconds(2000).unwrap(),
			16,
		);
		let (handle, committer) = build_committer_actor(&engine, group);
		let store = committer.operators.clone();

		let op_a = OperatorId(3);
		let op_b = OperatorId(4);
		let inner_a = state_inner(b"a");
		let inner_b = state_inner(b"b");
		let slice = state_slice(&[(op_a, &inner_a, 1), (op_b, &inner_b, 2)]);

		let replies = SliceReplies::new(1);
		assert!(handle
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply: replies.reply(0),
			})
			.is_ok());

		sleep(StdDuration::from_millis(300));
		assert_eq!(
			store.total_bytes(),
			ByteSize::ZERO,
			"operator state must not become visible in the store before the group flushes and \
			 the commit completes"
		);

		replies.wait();
		assert_eq!(
			replies.versions()[0].1,
			CommitVersion(0),
			"operator state is skipped on the way into the transaction, so a slice carrying nothing \
			 else commits empty and returns the discarded-commit sentinel; a real version here would \
			 mean the state also leaked into the multi store"
		);

		assert_eq!(
			store.get(op_a, &EncodedKey::new(inner_a.as_slice())),
			Some(EncodedOperatorRow::timeless(&[1; 4])),
			"the committed slice's state must be readable from the store"
		);
		assert_eq!(
			store.get(op_b, &EncodedKey::new(inner_b.as_slice())),
			Some(EncodedOperatorRow::timeless(&[2; 4]))
		);
	}
}
