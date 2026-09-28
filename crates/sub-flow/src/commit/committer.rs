// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem::take, sync::Arc};

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::{CommitVersion, SourceVersion},
	delta::RemoveVisibility,
	interface::{
		catalog::flow::FlowId,
		cdc::{CdcConsumerId, ConsumerClass},
		change::Change,
	},
	key::{any::TaggedKey, tag::KeyTag},
	return_internal_error,
};
#[cfg(test)]
use reifydb_engine::engine::StandardEngine;
use reifydb_flow_async::transaction::substrate::{apply_operator_state, apply_operator_state_with_checkpoints};
use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	commit::{CommitApply, CommitCompletion, CommitHandle, CommitSubmission},
	transaction::command::CommandTransaction,
};
#[cfg(test)]
use reifydb_value::value::identity::IdentityId;
use reifydb_value::{Result, util::hex::display as hex_display};
use tracing::instrument;

use crate::{commit::quiescence::FlowMaterialization, progress::tracker::FlowPositionTracker};

pub type CommitterHandle = ActorHandle<CommitterMessage>;

const COMMITTER_BATCH_SIZE: usize = 1024;

pub(crate) type SliceCommitReply = Box<dyn FnOnce(Result<CommitVersion>) + Send>;
pub(crate) type TickCommitReply = Box<dyn FnOnce(Result<()>) + Send>;

pub enum CommitterMessage {
	Slice {
		slice: FlowSlice,
		reply: SliceCommitReply,
	},

	Tick {
		flow_id: FlowId,
		source: SourceVersion,
		pending: Pending,
		view_changes: Vec<Change>,
		reply: TickCommitReply,
	},

	Flush,
}

pub struct CommitterActor {
	committer: Committer,
	commit: CommitHandle,
	name: &'static str,
}

pub struct CommitterState {
	groups: BTreeMap<SourceVersion, Vec<(FlowSlice, SliceCommitReply)>>,
	flush_queued: bool,
}

impl CommitterActor {
	pub fn new(committer: Committer, commit: CommitHandle) -> Self {
		Self {
			committer,
			commit,
			name: "flow-committer",
		}
	}

	pub fn named(mut self, name: &'static str) -> Self {
		self.name = name;
		self
	}

	fn enqueue(
		&self,
		state: &mut CommitterState,
		ctx: &Context<CommitterMessage>,
		source: SourceVersion,
		slice: FlowSlice,
		reply: SliceCommitReply,
	) {
		state.groups.entry(source).or_default().push((slice, reply));
		if state.flush_queued {
			return;
		}
		if ctx.self_ref().send(CommitterMessage::Flush).is_ok() {
			state.flush_queued = true;
		} else {
			self.flush(state);
		}
	}

	fn flush(&self, state: &mut CommitterState) {
		if state.groups.is_empty() {
			return;
		}
		self.flush_groups(take(&mut state.groups));
	}

	#[instrument(name = "flow::committer::flush", level = "debug", skip_all, fields(
		committer = self.name,
		groups = groups.len(),
		slices = groups.values().map(Vec::len).sum::<usize>()
	))]
	fn flush_groups(&self, groups: BTreeMap<SourceVersion, Vec<(FlowSlice, SliceCommitReply)>>) {
		for (_, group) in groups {
			let (slices, replies): (Vec<FlowSlice>, Vec<SliceCommitReply>) = group.into_iter().unzip();
			self.committer.commit_slices(
				self.name,
				&self.commit,
				Arc::new(slices),
				replies.into_iter().enumerate().collect(),
			);
		}
	}

	#[instrument(name = "flow::committer::submit_tick", level = "debug", skip_all, fields(committer = self.name))]
	fn submit_tick(
		&self,
		flow_id: FlowId,
		source: SourceVersion,
		pending: Pending,
		view_changes: Vec<Change>,
		reply: TickCommitReply,
	) {
		let pending = Arc::new(pending);

		let apply_committer = self.committer.clone();
		let apply_pending = Arc::clone(&pending);
		let apply: CommitApply = Box::new(move |transaction| {
			apply_committer.apply_tick(transaction, &apply_pending, view_changes, source)
		});

		let completion_committer = self.committer.clone();
		let completion: CommitCompletion = Box::new(move |result| match result {
			Ok(version) => {
				apply_operator_state(&completion_committer.operators, &pending);
				completion_committer.materialization.record_output(version);
				completion_committer.flow_tracker.record_commit(flow_id, version);
				(reply)(Ok(()));
			}
			Err(e) => (reply)(Err(e)),
		});

		self.commit.submit(CommitSubmission {
			apply,
			completion,
		});
	}
}

impl Actor for CommitterActor {
	type State = CommitterState;
	type Message = CommitterMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		CommitterState {
			groups: BTreeMap::new(),
			flush_queued: false,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CommitterMessage::Slice {
				slice,
				reply,
			} => match slice.source {
				Some(source) => self.enqueue(state, ctx, source, slice, reply),
				None => {
					self.flush(state);
					self.committer.commit_slices(
						self.name,
						&self.commit,
						Arc::new(vec![slice]),
						vec![(0, reply)],
					);
				}
			},
			CommitterMessage::Tick {
				flow_id,
				source,
				pending,
				view_changes,
				reply,
			} => {
				self.flush(state);
				self.submit_tick(flow_id, source, pending, view_changes, reply)
			}
			CommitterMessage::Flush => {
				state.flush_queued = false;
				self.flush(state);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().batch_size(COMMITTER_BATCH_SIZE)
	}
}

pub struct FlowSlice {
	pub combined: Pending,

	pub checkpoints: Vec<(FlowId, CommitVersion)>,

	pub checkpoint_deletes: Vec<FlowId>,

	pub view_changes: Vec<Change>,

	pub control_cursor: Option<(CdcConsumerId, CommitVersion)>,

	pub source: Option<SourceVersion>,
}

impl FlowSlice {
	pub fn empty() -> Self {
		Self {
			combined: Pending::new(),
			checkpoints: Vec::new(),
			checkpoint_deletes: Vec::new(),
			view_changes: Vec::new(),
			control_cursor: None,
			source: None,
		}
	}

	fn produced_output(&self) -> bool {
		self.combined.iter_sorted().next().is_some() || !self.view_changes.is_empty()
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

	#[instrument(name = "flow::committer::commit_slices", level = "debug", skip_all, fields(
		committer = name,
		slices = members.len()
	))]
	fn commit_slices(
		&self,
		name: &'static str,
		commit: &CommitHandle,
		slices: Arc<Vec<FlowSlice>>,
		members: Vec<(usize, SliceCommitReply)>,
	) {
		self.submit_slices(name, commit, slices, members)
	}

	#[instrument(name = "flow::committer::retry_slice", level = "debug", skip_all, fields(committer = name))]
	fn retry_slice(
		&self,
		name: &'static str,
		commit: &CommitHandle,
		slices: Arc<Vec<FlowSlice>>,
		member: (usize, SliceCommitReply),
	) {
		self.submit_slices(name, commit, slices, vec![member])
	}

	fn submit_slices(
		&self,
		name: &'static str,
		commit: &CommitHandle,
		slices: Arc<Vec<FlowSlice>>,
		members: Vec<(usize, SliceCommitReply)>,
	) {
		let indices: Vec<usize> = members.iter().map(|(index, _)| *index).collect();

		let apply_committer = self.clone();
		let apply_slices = Arc::clone(&slices);
		let apply: CommitApply = Box::new(move |transaction| {
			for index in indices {
				apply_committer.apply_slice(transaction, &apply_slices[index])?;
			}
			Ok(())
		});

		let completion_committer = self.clone();
		let retry_commit = commit.clone();
		let completion: CommitCompletion = Box::new(move |result| match result {
			Ok(version) => {
				for (index, reply) in members {
					(reply)(completion_committer.finish_slice(&slices[index], version));
				}
				release_slices(slices);
			}
			Err(e) => match <[(usize, SliceCommitReply); 1]>::try_from(members) {
				Ok([(_, reply)]) => (reply)(Err(e)),
				Err(members) => {
					for member in members {
						completion_committer.retry_slice(
							name,
							&retry_commit,
							Arc::clone(&slices),
							member,
						);
					}
				}
			},
		});

		commit.submit(CommitSubmission {
			apply,
			completion,
		});
	}

	#[instrument(name = "flow::committer::apply_slice", level = "debug", skip_all)]
	fn apply_slice(&self, transaction: &mut CommandTransaction, slice: &FlowSlice) -> Result<()> {
		if let Some(source) = slice.source {
			transaction.stamp_source(source)?;
		}
		apply_pending_writes(transaction, &slice.combined)?;

		for change in &slice.view_changes {
			transaction.track_flow_change(change.clone());
		}

		if let Some((consumer_id, version)) = &slice.control_cursor {
			CdcCheckpoint::persist(transaction, consumer_id, *version, ConsumerClass::Pinning)?;
		}

		Ok(())
	}

	#[instrument(name = "flow::committer::finish_slice", level = "trace", skip_all, fields(
		write_count = slice.combined.len(),
		checkpoint_count = slice.checkpoints.len()
	))]
	fn finish_slice(&self, slice: &FlowSlice, version: CommitVersion) -> Result<CommitVersion> {
		apply_operator_state_with_checkpoints(
			&self.operators,
			&slice.combined,
			&slice.checkpoints,
			&slice.checkpoint_deletes,
		)?;
		if slice.produced_output() {
			self.materialization.record_output(version);
		}
		self.post_commit_slice(version, &slice.checkpoints, &slice.checkpoint_deletes);
		Ok(version)
	}

	#[instrument(name = "flow::committer::post_commit_slice", level = "trace", skip_all, fields(
		checkpoint_count = checkpoints.len()
	))]
	fn post_commit_slice(
		&self,
		commit: CommitVersion,
		checkpoints: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
	) {
		for (flow_id, version) in checkpoints {
			self.flow_tracker.update_committed(*flow_id, *version, commit);
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
		source: SourceVersion,
	) -> Result<()> {
		transaction.stamp_source(source)?;
		apply_pending_writes(transaction, pending)?;

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		Ok(())
	}
}

#[instrument(name = "flow::committer::release_slices", level = "trace", skip_all)]
fn release_slices(slices: Arc<Vec<FlowSlice>>) {
	drop(slices);
}

#[instrument(name = "flow::committer::apply_pending", level = "debug", skip_all)]
fn apply_pending_writes(transaction: &mut CommandTransaction, combined: &Pending) -> Result<()> {
	for (encoded, pw) in combined.iter_ordered() {
		if matches!(KeyTag::of(encoded), Some(KeyTag::OperatorState)) {
			continue;
		}
		let Some(key) = TaggedKey::decode(encoded) else {
			return_internal_error!(
				"flow pending write carries a key no typed key decodes: {}",
				hex_display(encoded.as_ref())
			)
		};
		match pw {
			PendingWrite::Set(value) => transaction.set(&key, value.clone())?,
			PendingWrite::Remove {
				announce: RemoveVisibility::Announced,
			} => {
				if matches!(key, TaggedKey::Row(_) | TaggedKey::SeriesRow(_)) {
					match transaction.get(&key)? {
						Some(existing) => transaction.remove_with_pre(&key, existing.bytes)?,
						None => transaction.remove(&key)?,
					}
				} else {
					transaction.remove(&key)?;
				}
			}
			PendingWrite::Remove {
				announce: RemoveVisibility::Unobserved,
			} => {
				if matches!(key, TaggedKey::Row(_) | TaggedKey::SeriesRow(_)) {
					match transaction.get(&key)? {
						Some(existing) => {
							transaction.remove_unobserved_with_pre(&key, existing.bytes)?
						}
						None => transaction.remove_unobserved(&key)?,
					}
				} else {
					transaction.remove_unobserved(&key)?;
				}
			}
			PendingWrite::Remove {
				announce: RemoveVisibility::Silent,
			} => transaction.remove_silent(&key)?,
		}
	}
	Ok(())
}

#[cfg(test)]
mod commit_integration {
	use std::sync::{
		atomic::{AtomicUsize, Ordering},
		mpsc,
	};

	use reifydb_cdc::consume::watermark::CdcConsumerWatermark;
	use reifydb_codec::{
		key::encoded::EncodedKey,
		row::{bytes::EncodedBytes, pod::EncodedPodRow},
	};
	use reifydb_core::{
		interface::catalog::{
			flow::OperatorId,
			id::{IndexId, TableId},
			object::ObjectId,
		},
		internal_error,
		key::{
			any::TaggedKey,
			catalog::IndexEntryKey,
			cdc::CdcConsumerKey,
			operator::state::{GroupStateKey, OperatorStateKey, unmanaged_key},
		},
		value::index::encoded::EncodedIndexKey,
	};
	use reifydb_runtime::sync::{mutex::Mutex, waiter::WaiterHandle};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{commit::CommitBegin, multi::RangeScope, transaction::Transaction};
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::duration::Duration};

	use super::*;

	struct SliceReplies {
		results: Mutex<Vec<(usize, Result<CommitVersion>)>>,
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
				.map(|(i, r)| (*i, *r.as_ref().expect("expected committed slice")))
				.collect()
		}
	}

	fn synthetic_key(index: u64) -> EncodedKey {
		// IndexEntry is neither CDC-excluded nor consumer-relevant: the producer records
		// the write while every consumer ignores it.
		IndexEntryKey::new(
			ObjectId::Table(TableId(1)),
			IndexId::primary(1u64),
			EncodedIndexKey::new([index as u8]),
		)
		.encode()
	}

	fn synthetic_slice(index: u64) -> FlowSlice {
		let mut combined = Pending::new();
		combined.insert(synthetic_key(index), EncodedBytes(CowVec::new(vec![index as u8; 4])));
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(index), CommitVersion(100 + index))];
		slice
	}

	fn build_committer_actor(engine: &StandardEngine, commit: CommitHandle) -> (CommitterHandle, Committer) {
		let tracker = FlowPositionTracker::new();
		let committer = Committer::new(
			tracker.clone(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
		);
		let handle = engine
			.spawner()
			.spawn_flow("commit-test-committer", CommitterActor::new(committer.clone(), commit));
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
	fn each_slice_commits_in_its_own_version() {
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let (handle, committer) = build_committer_actor(&engine, CommitHandle::new(begin));

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
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let (handle, committer) = build_committer_actor(&engine, CommitHandle::new(begin));

		let replies = SliceReplies::new(2);
		send_slices(&handle, &replies, 2);
		replies.wait();

		let mut query = engine.begin_query(IdentityId::system()).expect("begin query");
		let mut consumers: Vec<String> = Vec::new();
		for multi in Transaction::Query(&mut query)
			.range(CdcConsumerKey::full_scan(), RangeScope::All, 1024)
			.expect("scan consumer checkpoints")
		{
			let multi = multi.expect("consumer checkpoint row");
			if let TaggedKey::CdcConsumer(key) = &multi.key {
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
				committer.operators.checkpoint_get(FlowId(flow)).unwrap(),
				Some(CommitVersion(100 + flow)),
				"the checkpoint must instead land in the operator store, where the flush writes it in \
				 the same transaction as the state it belongs to"
			);
		}
	}

	fn state_inner(suffix: &[u8]) -> GroupStateKey {
		unmanaged_key(suffix).expect("a fixture name must fit the keyspace's id width").into()
	}

	fn state_slice(entries: &[(OperatorId, &GroupStateKey, u8)]) -> FlowSlice {
		let mut combined = Pending::new();
		for (operator, inner, tag) in entries {
			let (group, keyspace, suffix) = OperatorStateKey::decode_inner(inner.as_slice())
				.expect("test fixture group-state key must decode");
			let key = OperatorStateKey::encoded(*operator, group, keyspace, suffix);
			combined.classify(key.clone(), None);
			combined.insert(key, EncodedPodRow::new(&[*tag; 4]).into_bytes());
		}
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(1), CommitVersion(10))];
		slice
	}

	#[test]
	fn a_failed_commit_leaves_the_operator_state_untouched() {
		// Operator state is written in the completion, so a failed commit must write none of it.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin: CommitBegin = Arc::new(|| Err(internal_error!("commit could not begin")));
		let (handle, committer) = build_committer_actor(&engine, CommitHandle::new(begin));
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
		replies.wait();

		{
			let results = replies.results.lock();
			assert!(results[0].1.is_err(), "a commit that cannot begin must fail the slice commit");
		}
		assert_eq!(
			store.state_get(operator, &GroupStateKey::bound_unchecked(EncodedKey::new(inner.as_slice())))
				.unwrap(),
			None,
			"a failed commit must not leak its operator-state writes into the store"
		);
		assert_eq!(
			store.total_bytes().unwrap(),
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
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let (handle, committer) = build_committer_actor(&engine, CommitHandle::new(begin));
		let store = committer.operators.clone();

		let op_a = OperatorId(3);
		let op_b = OperatorId(4);
		let inner_a = state_inner(b"a");
		let inner_b = state_inner(b"b");
		let slice = state_slice(&[(op_a, &inner_a, 1), (op_b, &inner_b, 2)]);

		assert_eq!(
			store.total_bytes().unwrap(),
			ByteSize::ZERO,
			"precondition: the store must be empty before the slice is submitted, or the post-commit read proves nothing"
		);

		let replies = SliceReplies::new(1);
		assert!(handle
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply: replies.reply(0),
			})
			.is_ok());

		replies.wait();
		assert_eq!(
			replies.versions()[0].1,
			CommitVersion(0),
			"operator state is skipped on the way into the transaction, so a slice carrying nothing \
			 else commits empty and returns the discarded-commit sentinel; a real version here would \
			 mean the state also leaked into the multi store"
		);

		assert_eq!(
			store.state_get(op_a, &GroupStateKey::bound_unchecked(EncodedKey::new(inner_a.as_slice())))
				.unwrap(),
			Some(EncodedPodRow::new(&[1; 4])),
			"the committed slice's state must be readable from the store"
		);
		assert_eq!(
			store.state_get(op_b, &GroupStateKey::bound_unchecked(EncodedKey::new(inner_b.as_slice())))
				.unwrap(),
			Some(EncodedPodRow::new(&[2; 4]))
		);
	}

	fn gated_committer_actor(engine: &StandardEngine) -> (CommitterHandle, Committer, mpsc::Sender<()>) {
		let (release, gate) = mpsc::channel::<()>();
		let gate = Mutex::new(Some(gate));
		let begin_engine = engine.clone();
		let begin: CommitBegin = Arc::new(move || {
			let first = gate.lock().take();
			if let Some(gate) = first {
				gate.recv().expect("the test must release the first commit");
			}
			begin_engine.begin_command(IdentityId::system())
		});
		let (handle, committer) = build_committer_actor(engine, CommitHandle::new(begin));
		(handle, committer, release)
	}

	fn send_slice(handle: &CommitterHandle, slice: FlowSlice, reply: SliceCommitReply) {
		let sent = handle
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply,
			})
			.is_ok();
		assert!(sent, "send slice");
	}

	fn sourced_slice(index: u64, source: u64) -> FlowSlice {
		let mut slice = synthetic_slice(index);
		slice.source = Some(SourceVersion(source));
		slice
	}

	#[test]
	fn slices_of_one_source_queued_together_commit_in_one_version() {
		// Queued slices of one source must share a commit, or every flow pays its own commit.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let (handle, committer, release) = gated_committer_actor(&engine);

		let replies = SliceReplies::new(4);
		send_slice(&handle, synthetic_slice(9), replies.reply(0));
		send_slice(&handle, sourced_slice(1, 7), replies.reply(1));
		send_slice(&handle, sourced_slice(2, 7), replies.reply(2));
		send_slice(&handle, sourced_slice(3, 7), replies.reply(3));
		release.send(()).expect("release the first commit");
		replies.wait();

		let versions: BTreeMap<usize, CommitVersion> = replies.versions().into_iter().collect();
		assert!(versions[&0] < versions[&1], "the gate slice must commit on its own first: {versions:?}");
		assert_eq!(versions[&1], versions[&2], "slices of one source must share a version: {versions:?}");
		assert_eq!(versions[&1], versions[&3], "slices of one source must share a version: {versions:?}");

		let tracked = committer.flow_tracker.all();
		for i in 1..=3u64 {
			assert_eq!(
				tracked.get(&FlowId(i)).copied(),
				Some(CommitVersion(100 + i)),
				"every slice in the group must still advance its own flow"
			);
		}

		let mut query = engine.begin_query(IdentityId::system()).expect("begin query");
		for i in 1..=3u64 {
			let key = TaggedKey::decode(&synthetic_key(i)).expect("the fixture key must decode");
			assert!(
				Transaction::Query(&mut query).get(&key).expect("read grouped write").is_some(),
				"every slice in the group must land its writes, not only the first"
			);
		}
	}

	#[test]
	fn a_group_queued_after_a_flush_still_commits() {
		// Every group must get its own flush, or slices queued after the first flush never commit.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let (handle, committer) = build_committer_actor(&engine, CommitHandle::new(begin));

		for (round, source) in [(1u64, 7u64), (2, 8), (3, 8)] {
			let replies = SliceReplies::new(1);
			send_slice(&handle, sourced_slice(round, source), replies.reply(0));
			replies.wait();
			assert!(replies.versions()[0].1 > CommitVersion(0), "round {round} must commit its write");
		}
		assert_eq!(committer.flow_tracker.all().get(&FlowId(3)).copied(), Some(CommitVersion(103)));
	}

	#[test]
	fn slices_of_different_sources_never_share_a_version() {
		// A commit carries exactly one source stamp, so slices of different sources must never share one.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let (handle, _committer, release) = gated_committer_actor(&engine);

		let replies = SliceReplies::new(4);
		send_slice(&handle, synthetic_slice(9), replies.reply(0));
		send_slice(&handle, sourced_slice(1, 7), replies.reply(1));
		send_slice(&handle, sourced_slice(2, 8), replies.reply(2));
		send_slice(&handle, sourced_slice(3, 7), replies.reply(3));
		release.send(()).expect("release the first commit");
		replies.wait();

		let versions: BTreeMap<usize, CommitVersion> = replies.versions().into_iter().collect();
		assert_eq!(
			versions[&1], versions[&3],
			"slices of source 7 must still group around source 8: {versions:?}"
		);
		assert_ne!(versions[&1], versions[&2], "source 8 must not join the source 7 commit: {versions:?}");
	}

	#[test]
	fn a_checkpoint_delete_commits_after_the_slices_queued_before_it() {
		// A delete that overtakes its flow's queued slice lets that slice write the stopped flow back.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let (handle, committer, release) = gated_committer_actor(&engine);

		let mut delete = FlowSlice::empty();
		delete.checkpoint_deletes.push(FlowId(1));

		let replies = SliceReplies::new(3);
		send_slice(&handle, synthetic_slice(9), replies.reply(0));
		send_slice(&handle, sourced_slice(1, 7), replies.reply(1));
		send_slice(&handle, delete, replies.reply(2));
		release.send(()).expect("release the first commit");
		replies.wait();
		replies.versions();

		assert_eq!(
			committer.flow_tracker.all().get(&FlowId(1)).copied(),
			None,
			"the deleted flow must not come back in the tracker"
		);
		assert_eq!(
			committer.operators.checkpoint_get(FlowId(1)).unwrap(),
			None,
			"the deleted flow must not come back in the operator store"
		);
	}

	#[test]
	fn a_slice_that_cannot_commit_fails_alone_and_its_group_still_commits() {
		// One bad slice must not fail the healthy flows grouped with it, or they get poisoned for its error.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let (handle, committer, release) = gated_committer_actor(&engine);

		let mut bad = sourced_slice(2, 7);
		bad.combined.insert(EncodedKey::new([]), EncodedBytes(CowVec::new(vec![1u8; 4])));

		let replies = SliceReplies::new(4);
		send_slice(&handle, synthetic_slice(9), replies.reply(0));
		send_slice(&handle, sourced_slice(1, 7), replies.reply(1));
		send_slice(&handle, bad, replies.reply(2));
		send_slice(&handle, sourced_slice(3, 7), replies.reply(3));
		release.send(()).expect("release the first commit");
		replies.wait();

		{
			let results = replies.results.lock();
			let result = |index: usize| &results.iter().find(|(i, _)| *i == index).expect("reply").1;
			assert!(result(2).is_err(), "the slice with an undecodable key must fail");
			for healthy in [1, 3] {
				assert!(
					matches!(result(healthy), Ok(version) if *version > CommitVersion(0)),
					"a healthy slice grouped with a bad one must still commit"
				);
			}
		}

		let tracked = committer.flow_tracker.all();
		assert_eq!(tracked.get(&FlowId(1)).copied(), Some(CommitVersion(101)));
		assert_eq!(tracked.get(&FlowId(3)).copied(), Some(CommitVersion(103)));
		assert_eq!(tracked.get(&FlowId(2)).copied(), None, "the failed slice must not advance its flow");
	}
}
