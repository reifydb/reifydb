// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::flow::FlowId,
		cdc::{CdcConsumerId, ConsumerClass},
		change::Change,
	},
	key::{Key, cdc_consumer::FlowSnapshotPin, kind::KeyKind},
	state::budget::OperatorStateBudgetHandle,
};
#[cfg(test)]
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::transaction::substrate::apply_operator_state;
use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_store_operator::OperatorStore;
use reifydb_transaction::{
	group::{GroupCommitApply, GroupCommitCompletion, GroupCommitHandle, GroupCommitSubmission},
	transaction::{Transaction, command::CommandTransaction},
};
#[cfg(test)]
use reifydb_value::value::identity::IdentityId;
use reifydb_value::{Result, byte_size::ByteSize};
use tracing::{instrument, warn};

use crate::{
	catalog::FlowCatalog,
	deferred::{quiescence::FlowMaterialization, snapshot::SnapshotPinTracker, tracker::FlowPositionTracker},
};

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
		view_changes: Vec<Change>,
		reply: TickCommitReply,
	},
}

pub struct CommitterActor {
	committer: Committer,
	group: GroupCommitHandle,
	state_budget: OperatorStateBudgetHandle,
}

impl CommitterActor {
	pub fn new(committer: Committer, group: GroupCommitHandle, state_budget: OperatorStateBudgetHandle) -> Self {
		Self {
			committer,
			group,
			state_budget,
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
			snapshot_pins,
		} = slice;
		let produced_output = combined.iter_sorted().next().is_some()
			|| !view_changes.is_empty()
			|| !pending_shapes.is_empty();
		let combined = Arc::new(combined);

		let in_flight = pending_bytes(&combined);
		self.state_budget.charge_in_flight(in_flight);
		let completion_budget = self.state_budget.clone();

		let apply_committer = self.committer.clone();
		let apply_combined = Arc::clone(&combined);
		let apply_checkpoints = checkpoints.clone();
		let apply_deletes = checkpoint_deletes.clone();
		let apply_pins = snapshot_pins.clone();
		let apply: GroupCommitApply = Box::new(move |transaction| {
			apply_committer.apply_slice(
				transaction,
				&apply_combined,
				pending_shapes,
				&apply_checkpoints,
				&apply_deletes,
				view_changes,
				&control_cursor,
				&apply_pins,
			)
		});

		let completion_committer = self.committer.clone();
		let completion: GroupCommitCompletion = Box::new(move |result| {
			completion_budget.release_in_flight(in_flight);
			match result {
				Ok(version) => {
					apply_operator_state(&completion_committer.operators, version, &combined);
					if produced_output {
						completion_committer.materialization.record_output(version);
					}
					completion_committer.post_commit_slice(
						&checkpoints,
						&positions,
						&checkpoint_deletes,
						&snapshot_pins,
					);
					let combined =
						Arc::try_unwrap(combined).unwrap_or_else(|shared| (*shared).clone());
					(reply)(Ok((version, combined)));
				}
				Err(e) => (reply)(Err(e)),
			}
		});

		self.group.submit(GroupCommitSubmission {
			apply,
			completion,
		});
	}

	fn submit_tick(
		&self,
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
		reply: TickCommitReply,
	) {
		let pending = Arc::new(pending);

		let in_flight = pending_bytes(&pending);
		self.state_budget.charge_in_flight(in_flight);
		let completion_budget = self.state_budget.clone();

		let apply_committer = self.committer.clone();
		let apply_pending = Arc::clone(&pending);
		let apply: GroupCommitApply = Box::new(move |transaction| {
			apply_committer.apply_tick(transaction, &apply_pending, pending_shapes, view_changes)
		});

		let completion_committer = self.committer.clone();
		let completion: GroupCommitCompletion = Box::new(move |result| {
			completion_budget.release_in_flight(in_flight);
			match result {
				Ok(version) => {
					apply_operator_state(&completion_committer.operators, version, &pending);
					completion_committer.materialization.record_output(version);
					let pending =
						Arc::try_unwrap(pending).unwrap_or_else(|shared| (*shared).clone());
					(reply)(Some((version, pending)));
				}
				Err(e) => {
					warn!(error = %e, "failed to commit tick writes");
					(reply)(None);
				}
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
				view_changes,
				reply,
			} => self.submit_tick(pending, pending_shapes, view_changes, reply),
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

	pub snapshot_pins: Vec<(FlowId, CommitVersion)>,
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
			snapshot_pins: Vec::new(),
		}
	}
}

#[derive(Clone)]
pub struct Committer {
	catalog: FlowCatalog,
	flow_tracker: FlowPositionTracker,
	materialization: FlowMaterialization,
	operators: OperatorStore,
	snapshot_pins: SnapshotPinTracker,
}

impl Committer {
	pub fn new(
		catalog: FlowCatalog,
		flow_tracker: FlowPositionTracker,
		materialization: FlowMaterialization,
		operators: OperatorStore,
		snapshot_pins: SnapshotPinTracker,
	) -> Self {
		Self {
			catalog,
			flow_tracker,
			materialization,
			operators,
			snapshot_pins,
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
		snapshot_pins: &[(FlowId, CommitVersion)],
	) -> Result<()> {
		apply_pending_writes(transaction, combined)?;

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		for (flow_id, version) in checkpoints {
			CdcCheckpoint::persist(transaction, flow_id, *version, ConsumerClass::Pinning)?;
		}

		for (flow_id, version) in snapshot_pins {
			CdcCheckpoint::persist(transaction, &FlowSnapshotPin(*flow_id), *version, ConsumerClass::Pinning)?;
		}

		for flow_id in checkpoint_deletes {
			CdcCheckpoint::delete(transaction, flow_id)?;
			CdcCheckpoint::delete(transaction, &FlowSnapshotPin(*flow_id))?;
		}

		if let Some((consumer_id, version)) = control_cursor {
			CdcCheckpoint::persist(transaction, consumer_id, *version, ConsumerClass::Pinning)?;
		}

		self.catalog.persist_pending_shapes(&mut Transaction::Command(transaction), pending_shapes)
	}

	fn post_commit_slice(
		&self,
		checkpoints: &[(FlowId, CommitVersion)],
		positions: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
		snapshot_pins: &[(FlowId, CommitVersion)],
	) {
		for (flow_id, version) in checkpoints.iter().chain(positions.iter()) {
			self.flow_tracker.update(*flow_id, *version);
		}

		for (flow_id, version) in checkpoints {
			self.snapshot_pins.record_checkpoint(*flow_id, *version);
		}

		for (flow_id, version) in snapshot_pins {
			self.snapshot_pins.record_pin(*flow_id, *version);
		}

		for flow_id in checkpoint_deletes {
			self.flow_tracker.remove(*flow_id);
			self.snapshot_pins.forget(*flow_id);
		}
	}

	#[instrument(name = "flow::committer::apply_tick", level = "debug", skip_all)]
	fn apply_tick(
		&self,
		transaction: &mut CommandTransaction,
		pending: &Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
	) -> Result<()> {
		apply_pending_writes(transaction, pending)?;

		for change in view_changes {
			transaction.track_flow_change(change);
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
			snapshot_pins,
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
			&snapshot_pins,
		)?;

		let commit_version = transaction.commit_unchecked()?;

		apply_operator_state(&self.operators, commit_version, &combined);
		self.post_commit_slice(&checkpoints, &positions, &checkpoint_deletes, &snapshot_pins);
		Ok((commit_version, combined))
	}
}

fn pending_bytes(pending: &Pending) -> ByteSize {
	let mut total = 0u64;
	for (key, write) in pending.iter_sorted() {
		total = total.saturating_add(key.len() as u64);
		if let PendingWrite::Set(row) = write {
			total = total.saturating_add(row.len() as u64);
		}
	}
	ByteSize::from_bytes(total)
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
				announce: true,
			} => {
				if matches!(Key::kind(key), Some(KeyKind::Row)) {
					match transaction.get(key)? {
						Some(existing) => transaction.remove_with_pre(key, existing.row)?,
						None => transaction.remove(key)?,
					}
				} else {
					transaction.remove(key)?;
				}
			}
			PendingWrite::Remove {
				announce: false,
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

	use reifydb_cdc::consume::watermark::{CdcConsumerWatermark, compute_pinning_watermark};
	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_core::{
		interface::{catalog::flow::OperatorId, cdc::SystemChange},
		internal_error,
		key::{
			operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey},
			operator_state::OperatorStateKey,
		},
	};
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
		let committer = Committer::new(
			FlowCatalog::new(engine.catalog()),
			tracker.clone(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
			SnapshotPinTracker::new(),
		);
		let handle = engine.spawner().spawn_flow(
			"group-commit-test-committer",
			CommitterActor::new(committer.clone(), group, OperatorStateBudgetHandle::default()),
		);
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

	fn state_inner(suffix: &[u8]) -> GroupStateKey {
		OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::FIRST_CUSTOM, suffix)
	}

	fn state_slice(entries: &[(OperatorId, &GroupStateKey, u8)]) -> FlowSlice {
		let mut combined = Pending::new();
		for (operator, inner, tag) in entries {
			combined.insert(
				OperatorStateKey::encoded(*operator, inner.as_slice()),
				EncodedRow(CowVec::new(vec![*tag; 4])),
			);
		}
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(1), CommitVersion(10))];
		slice
	}

	#[test]
	fn a_failed_group_commit_leaves_the_arena_untouched() {
		// A rolled-back group must leave no arena state: otherwise flows read versions that
		// never became durable. Falsified by applying arena writes on the failure side or
		// inside the apply closure.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		// max_entries = 2 flushes when the poison joins the slice; slice first so its apply
		// runs before the sibling fails the group.
		let group =
			GroupCommitHandle::spawn(&engine.spawner(), begin, Duration::from_seconds(5).unwrap(), 2);
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
			"a failed commit must not leak its operator-state writes into the arena"
		);
		assert_eq!(store.upper(operator), CommitVersion(0), "a failed commit must not move upper");
		assert_eq!(store.total_bytes(), 0, "the arena must be byte-for-byte untouched");
	}

	#[test]
	fn arena_state_becomes_visible_only_with_the_commit_and_carries_upper() {
		// Arena state must not appear before the commit completes (falsified by applying at
		// submission time) and upper must equal the commit version for touched operators only
		// (falsified by skipping set_upper).
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
			0,
			"operator state must not become visible in the arena before the group flushes and \
			 the commit completes"
		);

		replies.wait();
		let version = replies.versions()[0].1;
		assert!(version > CommitVersion(0));

		assert_eq!(
			store.get(op_a, &EncodedKey::new(inner_a.as_slice())),
			Some(EncodedRow(CowVec::new(vec![1; 4]))),
			"the committed slice's state must be readable from the arena"
		);
		assert_eq!(
			store.get(op_b, &EncodedKey::new(inner_b.as_slice())),
			Some(EncodedRow(CowVec::new(vec![2; 4])))
		);
		assert_eq!(store.upper(op_a), version, "upper must track the commit version for touched operators");
		assert_eq!(store.upper(op_b), version);
		assert_eq!(
			store.upper(OperatorId(5)),
			CommitVersion(0),
			"an operator this slice never touched must keep its previous upper"
		);
	}

	#[test]
	fn a_snapshot_pin_rides_the_commit_as_a_pinning_consumer() {
		// The snapshot pin only protects replay if CDC truncation cannot pass it, and
		// truncation honors nothing but ConsumerClass::Pinning rows: compute_pinning_watermark
		// skips Ephemeral rows entirely. The pin must land under its derived consumer key at
		// exactly min(upper) and pull the pinning watermark down below the flow's own
		// checkpoint. Falsified by persisting the pin as Ephemeral (the watermark assertion
		// then reads 9) or by writing it under the flow's regular checkpoint key.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let committer = Committer::new(
			FlowCatalog::new(engine.catalog()),
			FlowPositionTracker::new(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
			SnapshotPinTracker::new(),
		);

		let mut slice = FlowSlice::empty();
		slice.checkpoints = vec![(FlowId(1), CommitVersion(9))];
		slice.snapshot_pins = vec![(FlowId(1), CommitVersion(7))];
		committer.commit_slice(&engine, slice).expect("commit slice with pin");

		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let pin = CdcCheckpoint::fetch_row(&mut Transaction::Query(&mut query), &FlowSnapshotPin(FlowId(1)))
			.expect("fetch pin")
			.expect("the pin row must exist under its derived consumer key");
		assert_eq!(pin.version, CommitVersion(7));
		assert_eq!(pin.class, ConsumerClass::Pinning, "an Ephemeral pin would not bound CDC truncation");

		let checkpoint =
			CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut query), &FlowId(1)).expect("fetch");
		assert_eq!(checkpoint, Some(CommitVersion(9)), "the pin must not overwrite the regular checkpoint");

		let watermark =
			compute_pinning_watermark(&mut Transaction::Query(&mut query)).expect("pinning watermark");
		assert_eq!(
			watermark,
			Some(CommitVersion(7)),
			"CDC truncation must be bounded by the pin, not the (higher) flow checkpoint"
		);
	}

	#[test]
	fn deleting_a_flows_checkpoint_also_deletes_its_snapshot_pin() {
		// Flow retirement cleans its checkpoint row; the snapshot pin must go with it, or a
		// dropped flow pins cdc.db forever and truncation never advances past its last
		// snapshot. The in-memory lag surface must forget the flow too. Falsified by removing
		// only the checkpoint on checkpoint_deletes or by leaving the tracker entry behind.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let pins = SnapshotPinTracker::new();
		let committer = Committer::new(
			FlowCatalog::new(engine.catalog()),
			FlowPositionTracker::new(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
			pins.clone(),
		);

		let mut slice = FlowSlice::empty();
		slice.checkpoints = vec![(FlowId(1), CommitVersion(9))];
		slice.snapshot_pins = vec![(FlowId(1), CommitVersion(7))];
		committer.commit_slice(&engine, slice).expect("commit slice with pin");
		assert_eq!(pins.lags(), vec![(FlowId(1), 2)], "precondition: the pin must be tracked before retire");

		let mut retire = FlowSlice::empty();
		retire.checkpoint_deletes = vec![FlowId(1)];
		committer.commit_slice(&engine, retire).expect("commit retire slice");

		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		assert_eq!(
			CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut query), &FlowId(1)).expect("fetch"),
			None,
			"the regular checkpoint must be deleted"
		);
		assert_eq!(
			CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut query), &FlowSnapshotPin(FlowId(1)))
				.expect("fetch"),
			None,
			"the snapshot pin must be deleted with the flow or it pins cdc.db forever"
		);
		assert!(pins.lags().is_empty(), "the retired flow must leave the lag surface");
	}
}
