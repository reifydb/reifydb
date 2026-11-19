// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::take,
	sync::Arc,
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::{CommitVersion, SourceVersion},
	delta::RemoveVisibility,
	interface::{
		catalog::{flow::FlowId, id::QueueId, object::ObjectId, storage::StorageId},
		operator::{FlowCheckpoint, OperatorSlice},
	},
	key::{any::TaggedKey, queue::QueueKey},
	return_internal_error,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow_async::transaction::{ViewLayers, substrate::operator_section};
use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_store_operator::{store::OperatorStore, types::classify_slice};
use reifydb_transaction::{
	change::{QueueRowInsertion, RowChange},
	commit::{Committed, DeferredFlowCommit},
};
#[cfg(test)]
use reifydb_value::value::identity::IdentityId;
use reifydb_value::{byte_size::ByteSize, error::ValueError, util::hex::display as hex_display};
use tracing::instrument;

use crate::{
	commit::quiescence::FlowMaterialization,
	marker::{InputMarkers, MarkerPlane, WrittenView},
	progress::tracker::{CommitOutput, FlowPositionTracker},
};

pub type CommitterHandle = ActorHandle<CommitterMessage>;

const COMMITTER_BATCH_SIZE: usize = 1024;

pub(crate) type SliceCommitReply = Box<dyn FnOnce(Result<CommitVersion, ValueError>) + Send>;
pub(crate) type TickCommitReply = Box<dyn FnOnce(Result<(), ValueError>) + Send>;

pub enum CommitterMessage {
	Slice {
		slice: FlowSlice,
		reply: SliceCommitReply,
	},

	Tick {
		flow_id: FlowId,
		members: Vec<FlowId>,
		source: SourceVersion,
		pending: Pending,
		owned_queues: Arc<BTreeSet<QueueId>>,
		reply: TickCommitReply,
	},

	Flush,
}

pub struct CommitterActor {
	committer: Committer,
	name: &'static str,
}

pub struct CommitterState {
	groups: BTreeMap<SourceVersion, Vec<(FlowSlice, SliceCommitReply)>>,
	flush_queued: bool,
}

impl CommitterActor {
	pub fn new(committer: Committer) -> Self {
		Self {
			committer,
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
				Arc::new(slices),
				replies.into_iter().enumerate().collect(),
			);
		}
	}

	#[instrument(name = "flow::committer::submit_tick", level = "debug", skip_all, fields(committer = self.name))]
	fn submit_tick(
		&self,
		flow_id: FlowId,
		members: Vec<FlowId>,
		source: SourceVersion,
		pending: Pending,
		owned_queues: Arc<BTreeSet<QueueId>>,
		reply: TickCommitReply,
	) {
		let (operator, sizes) = match operator_section(&self.committer.operators, &pending, &[], &[]) {
			Ok(section) => section,
			Err(e) => return (reply)(Err(e)),
		};

		let guard = self.committer.operators.begin_commit();
		let result = self.committer.commit_tick(&pending, source, operator.clone(), &owned_queues);
		match result {
			Ok(committed) => {
				self.committer.apply_operator_slice(&operator, &sizes, committed);
				let version = committed.version;
				self.committer.materialization.record_output(version);
				self.committer.flow_tracker.record_commit(flow_id, version);
				for member in &members {
					self.committer.flow_tracker.record_commit(*member, version);
				}
				(reply)(Ok(()));
			}
			Err(e) => (reply)(Err(e)),
		}
		drop(guard);
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
						Arc::new(vec![slice]),
						vec![(0, reply)],
					);
				}
			},
			CommitterMessage::Tick {
				flow_id,
				members,
				source,
				pending,
				owned_queues,
				reply,
			} => {
				self.flush(state);
				self.submit_tick(flow_id, members, source, pending, owned_queues, reply)
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

	pub checkpoints: Vec<(FlowId, FlowCheckpoint)>,

	pub checkpoint_deletes: Vec<FlowId>,

	pub emitted: bool,

	pub source: Option<SourceVersion>,

	pub written_views: Vec<WrittenView>,

	pub input_markers: Option<InputMarkers>,

	pub layers: Vec<(SourceVersion, Pending)>,

	pub owned_queues: Arc<BTreeSet<QueueId>>,
}

impl FlowSlice {
	pub fn empty() -> Self {
		Self {
			combined: Pending::new(),
			checkpoints: Vec::new(),
			checkpoint_deletes: Vec::new(),
			emitted: false,
			source: None,
			written_views: Vec::new(),
			input_markers: None,
			layers: Vec::new(),
			owned_queues: Arc::default(),
		}
	}

	fn produced_output(&self) -> bool {
		produced_output(&self.combined, self.emitted)
	}
}

pub(crate) fn produced_output(pending: &Pending, emitted: bool) -> bool {
	!pending.is_empty() || emitted
}

#[derive(Clone)]
pub struct Committer {
	engine: StandardEngine,
	flow_tracker: FlowPositionTracker,
	materialization: FlowMaterialization,
	operators: OperatorStore,
	markers: Option<MarkerPlane>,
}

impl Committer {
	pub fn new(
		engine: StandardEngine,
		flow_tracker: FlowPositionTracker,
		materialization: FlowMaterialization,
		operators: OperatorStore,
	) -> Self {
		Self {
			engine,
			flow_tracker,
			materialization,
			operators,
			markers: None,
		}
	}

	pub(crate) fn with_markers(mut self, markers: Option<MarkerPlane>) -> Self {
		self.markers = markers;
		self
	}

	#[instrument(name = "flow::committer::commit_slices", level = "debug", skip_all, fields(
		committer = name,
		slices = members.len()
	))]
	fn commit_slices(
		&self,
		name: &'static str,
		slices: Arc<Vec<FlowSlice>>,
		members: Vec<(usize, SliceCommitReply)>,
	) {
		self.submit_slices(name, slices, members)
	}

	#[instrument(name = "flow::committer::retry_slice", level = "debug", skip_all, fields(committer = name))]
	fn retry_slice(&self, name: &'static str, slices: Arc<Vec<FlowSlice>>, member: (usize, SliceCommitReply)) {
		self.submit_slices(name, slices, vec![member])
	}

	fn submit_slices(
		&self,
		name: &'static str,
		slices: Arc<Vec<FlowSlice>>,
		members: Vec<(usize, SliceCommitReply)>,
	) {
		let mut sized = Vec::with_capacity(members.len());
		for (index, reply) in members {
			let slice = &slices[index];
			match operator_section(
				&self.operators,
				&slice.combined,
				&slice.checkpoints,
				&slice.checkpoint_deletes,
			) {
				Ok((operator, sizes)) => sized.push((index, reply, operator, sizes)),
				Err(e) => (reply)(Err(e)),
			}
		}
		if sized.is_empty() {
			return;
		}
		let sections: Vec<(usize, OperatorSlice)> =
			sized.iter().map(|(index, _, operator, _)| (*index, operator.clone())).collect();

		let guard = self.operators.begin_commit();
		let result = self.commit_group(&slices, sections);
		match result {
			Ok(committed) => {
				for (index, reply, operator, sizes) in sized {
					(reply)(Ok(self.finish_slice(&slices[index], &operator, &sizes, committed)));
				}
				release_slices(slices);
			}
			Err(e) => match <[SizedSlice; 1]>::try_from(sized) {
				Ok([(_, reply, _, _)]) => (reply)(Err(e)),
				Err(sized) => {
					for (index, reply, _, _) in sized {
						self.retry_slice(name, Arc::clone(&slices), (index, reply));
					}
				}
			},
		}
		drop(guard);
	}

	fn commit_group(
		&self,
		slices: &[FlowSlice],
		sections: Vec<(usize, OperatorSlice)>,
	) -> Result<Committed, ValueError> {
		let mut commit = self.engine.begin_deferred_flow_commit()?;
		for (index, operator) in sections {
			self.apply_slice(&mut commit, &slices[index], operator)?;
		}
		commit.commit()
	}

	fn commit_tick(
		&self,
		pending: &Pending,
		source: SourceVersion,
		operator: OperatorSlice,
		owned_queues: &BTreeSet<QueueId>,
	) -> Result<Committed, ValueError> {
		let mut commit = self.engine.begin_deferred_flow_commit()?;
		self.apply_tick(&mut commit, pending, source, operator, owned_queues)?;
		commit.commit()
	}

	#[instrument(name = "flow::committer::apply_slice", level = "debug", skip_all)]
	fn apply_slice(
		&self,
		commit: &mut DeferredFlowCommit,
		slice: &FlowSlice,
		operator: OperatorSlice,
	) -> Result<(), ValueError> {
		commit.own_queues(slice.owned_queues.iter().copied());
		self.operators.check_checkpoints(&operator.checkpoints)?;
		push_if_not_empty(commit, operator);
		if slice.layers.len() < 2 {
			if let Some(source) = slice.source {
				commit.begin_layer(source);
			}
			apply_pending_writes(commit, &slice.combined)?;
		} else {
			for (source, layer) in &slice.layers {
				commit.begin_layer(*source);
				apply_pending_writes(commit, layer)?;
			}
		}
		if let Some(input) = &slice.input_markers {
			for written in &slice.written_views {
				commit.attach_view_markers(written.view, input.markers())?;
			}
		}
		Ok(())
	}

	#[instrument(name = "flow::committer::finish_slice", level = "trace", skip_all, fields(
		write_count = slice.combined.len(),
		checkpoint_count = slice.checkpoints.len()
	))]
	fn finish_slice(
		&self,
		slice: &FlowSlice,
		operator: &OperatorSlice,
		sizes: &[Option<ByteSize>],
		committed: Committed,
	) -> CommitVersion {
		self.apply_operator_slice(operator, sizes, committed);
		let version = committed.version;
		if let Some(markers) = &self.markers {
			markers.record(version, slice);
		}
		if slice.produced_output() {
			self.materialization.record_output(version);
		}
		let first = slice.layers.first().map(|(first, _)| *first);
		let output = match slice.layers.len() > 1 {
			false => CommitOutput::OneSource,
			true if slice
				.checkpoints
				.iter()
				.any(|(flow_id, _)| self.flow_tracker.is_lookup_producer(*flow_id)) =>
			{
				CommitOutput::of_layers(ViewLayers::from_pending(&slice.layers))
			}
			true => CommitOutput::PerSourceUntracked,
		};
		self.post_commit_slice(
			version,
			first,
			slice.source,
			output,
			&slice.checkpoints,
			&slice.checkpoint_deletes,
		);
		version
	}

	#[instrument(name = "flow::committer::apply_operator_slice", level = "trace", skip_all, fields(
		write_count = operator.writes.len(),
		checkpoint_count = operator.checkpoints.len()
	))]
	fn apply_operator_slice(&self, operator: &OperatorSlice, sizes: &[Option<ByteSize>], committed: Committed) {
		if operator.is_empty() {
			return;
		}
		let Some(lsn) = committed.lsn else {
			panic!(
				"an operator slice committed at version {} without a commit log lsn; its state was never logged",
				committed.version.0
			);
		};
		if let Err(e) = self.operators.apply_batch_with_checkpoints(
			&classify_slice(operator, sizes),
			&operator.checkpoints,
			&operator.deletes,
			Some(lsn),
		) {
			panic!("operator state for lsn {} is in the commit log but failed to apply: {e}", lsn.as_u64());
		}
	}

	#[instrument(name = "flow::committer::post_commit_slice", level = "trace", skip_all, fields(
		checkpoint_count = checkpoints.len()
	))]
	fn post_commit_slice(
		&self,
		commit: CommitVersion,
		first: Option<SourceVersion>,
		source: Option<SourceVersion>,
		output: CommitOutput,
		checkpoints: &[(FlowId, FlowCheckpoint)],
		checkpoint_deletes: &[FlowId],
	) {
		for (flow_id, checkpoint) in checkpoints {
			let version = checkpoint.version;
			let source = source.unwrap_or(SourceVersion(version.0));
			self.flow_tracker.update_committed_at(
				*flow_id,
				version,
				commit,
				first.unwrap_or(source),
				source,
				output.clone(),
			);
		}

		for flow_id in checkpoint_deletes {
			self.flow_tracker.remove(*flow_id);
		}
	}

	#[instrument(name = "flow::committer::apply_tick", level = "debug", skip_all)]
	fn apply_tick(
		&self,
		commit: &mut DeferredFlowCommit,
		pending: &Pending,
		source: SourceVersion,
		operator: OperatorSlice,
		owned_queues: &BTreeSet<QueueId>,
	) -> Result<(), ValueError> {
		commit.own_queues(owned_queues.iter().copied());
		commit.stamp_source(source);
		push_if_not_empty(commit, operator);
		apply_pending_writes(commit, pending)?;

		Ok(())
	}
}

type SizedSlice = (usize, SliceCommitReply, OperatorSlice, Vec<Option<ByteSize>>);

fn push_if_not_empty(commit: &mut DeferredFlowCommit, operator: OperatorSlice) {
	if operator.is_empty() {
		return;
	}
	commit.push_operator_slice(operator);
}

#[instrument(name = "flow::committer::release_slices", level = "trace", skip_all)]
fn release_slices(slices: Arc<Vec<FlowSlice>>) {
	drop(slices);
}

#[instrument(name = "flow::committer::apply_pending", level = "debug", skip_all)]
fn apply_pending_writes(commit: &mut DeferredFlowCommit, combined: &Pending) -> Result<(), ValueError> {
	let mut writes: Vec<(TaggedKey, &PendingWrite)> = Vec::with_capacity(combined.len());
	let dropped = dropped_queues(commit, combined)?;
	for (encoded, pw) in combined.rows_ordered() {
		let Some(key) = TaggedKey::decode(encoded) else {
			return_internal_error!(
				"flow pending write carries a key no typed key decodes: {}",
				hex_display(encoded.as_ref())
			)
		};
		if !writes_to_dropped_queue(&key, &dropped) {
			writes.push((key, pw));
		}
	}
	let mut sets: Vec<(TaggedKey, EncodedBytes)> = Vec::new();
	let mut removes: Vec<(TaggedKey, RemoveVisibility)> = Vec::new();
	for (key, pw) in writes {
		match pw {
			PendingWrite::Set(value) => {
				if !removes.is_empty() {
					commit.remove(take(&mut removes))?;
				}
				sets.push((key, value.clone()));
			}
			PendingWrite::Remove {
				announce,
			} => {
				if !sets.is_empty() {
					commit.set(take(&mut sets))?;
				}
				removes.push((key, *announce));
			}
		}
	}
	if !sets.is_empty() {
		commit.set(sets)?;
	}
	if !removes.is_empty() {
		commit.remove(removes)?;
	}
	commit.track_row_change(&queue_row_changes(combined, &dropped));
	Ok(())
}

fn dropped_queues(commit: &mut DeferredFlowCommit, combined: &Pending) -> Result<BTreeSet<QueueId>, ValueError> {
	let queues: BTreeSet<QueueId> = combined.queue_items().iter().map(|item| item.queue).collect();
	if queues.is_empty() {
		return Ok(BTreeSet::new());
	}
	let keys: Vec<TaggedKey> = queues.iter().map(|queue| QueueKey::new(*queue).into()).collect();
	let found = commit.get(&keys)?;
	Ok(queues.into_iter().zip(found).filter(|(_, row)| row.is_none()).map(|(queue, _)| queue).collect())
}

fn writes_to_dropped_queue(key: &TaggedKey, dropped: &BTreeSet<QueueId>) -> bool {
	match key {
		TaggedKey::Row(key) => matches!(key.storage, StorageId::Queue(queue) if dropped.contains(&queue)),
		TaggedKey::Partition(key) => matches!(key.object, ObjectId::Queue(queue) if dropped.contains(&queue)),
		_ => false,
	}
}

fn queue_row_changes(combined: &Pending, dropped: &BTreeSet<QueueId>) -> Vec<RowChange> {
	combined.queue_items()
		.iter()
		.filter(|item| !dropped.contains(&item.queue))
		.map(|item| {
			RowChange::QueueInsert(QueueRowInsertion {
				queue_id: item.queue,
				partition: item.partition,
				key_hash: item.key_hash,
				row_number: item.row_number,
				not_before: None,
				encoded: item.encoded.clone(),
			})
		})
		.collect()
}

#[cfg(test)]
mod commit_integration {
	use std::{
		ops::RangeInclusive,
		slice::from_ref,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
			mpsc,
		},
		time::Duration as StdDuration,
	};

	use reifydb_codec::{
		key::encoded::EncodedKey,
		row::{bytes::EncodedBytes, pod::EncodedPodRow},
	};
	use reifydb_core::{
		actors::pending::PendingQueueItem,
		commit::position::ReaderPosition,
		delta::{Delta, RemoveAnnounce, SetPre},
		interface::{
			catalog::{flow::OperatorId, id::ViewId},
			commit::CommitEntry,
			operator::OperatorStateWrite,
			store::WalFloorCheckpoint,
		},
		internal_error,
		key::{
			any::TaggedKey,
			operator::state::{GroupStateKey, OperatorStateKey, unmanaged_key},
			partition::PartitionKey,
			row::RowKey,
			series::SeriesRowKey,
			tag::KeyTag,
		},
		wal::{CommitRecord, WalRecord},
	};
	use reifydb_runtime::{
		context::{RuntimeContext, transaction::TransactionContext},
		sync::{mutex::Mutex, waiter::WaiterHandle},
	};
	use reifydb_store_multi::testing::MultiStoreHooks;
	use reifydb_store_operator::{
		actor::resident_flush::flush_now,
		config::{OperatorPersistentConfig, OperatorStoreConfig, ResidentConfig},
		error::StoreOperatorError,
		persistent::{PersistentTier, testing::PersistentHooks},
		resident::{Resident, ResidentLimits},
		types::OperatorWrite,
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		queue::scheduling::queue_partition_counters, transaction::Transaction, wal::CommitWal,
	};
	use reifydb_value::{
		byte_size::ByteSize,
		util::cowvec::CowVec,
		value::{duration::Duration, partition::Partition, row_number::RowNumber},
	};
	use reifydb_wal::{lsn::Lsn, sync::WalMode};

	use super::*;
	use crate::commit::next_lsn;

	fn system_ctx() -> Arc<TransactionContext> {
		Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
	}

	fn checkpoint_at(version: u64) -> FlowCheckpoint {
		FlowCheckpoint {
			version: CommitVersion(version),
			lsn: Lsn::new(version + 1_000),
		}
	}

	struct SliceReplies {
		results: Mutex<Vec<(usize, Result<CommitVersion, ValueError>)>>,
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

	struct FailFirstQueueRead {
		reads: AtomicUsize,
	}

	impl MultiStoreHooks for FailFirstQueueRead {
		fn before_get(&self, keys: &[EncodedKey], _version: CommitVersion) -> Result<(), ValueError> {
			if !keys.iter().any(|key| KeyTag::of(key) == Some(KeyTag::Queue)) {
				return Ok(());
			}
			if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
				return Err(internal_error!("the batch commit fails once"));
			}
			Ok(())
		}
	}

	struct BlockFirstRead {
		key: EncodedKey,
		gate: Mutex<Option<mpsc::Receiver<()>>>,
	}

	impl MultiStoreHooks for BlockFirstRead {
		fn before_get(&self, keys: &[EncodedKey], _version: CommitVersion) -> Result<(), ValueError> {
			if !keys.contains(&self.key) {
				return Ok(());
			}
			let first = self.gate.lock().take();
			if let Some(gate) = first {
				gate.recv().expect("the test must release the first commit");
			}
			Ok(())
		}
	}

	struct CountingReads {
		keys: BTreeSet<EncodedKey>,
		reads: AtomicUsize,
	}

	impl MultiStoreHooks for CountingReads {
		fn before_get(&self, keys: &[EncodedKey], _version: CommitVersion) -> Result<(), ValueError> {
			if keys.iter().any(|key| self.keys.contains(key)) {
				self.reads.fetch_add(1, Ordering::SeqCst);
			}
			Ok(())
		}
	}

	struct RecordedReads {
		calls: Mutex<Vec<(Vec<EncodedKey>, CommitVersion)>>,
	}

	impl MultiStoreHooks for RecordedReads {
		fn before_get(&self, keys: &[EncodedKey], version: CommitVersion) -> Result<(), ValueError> {
			self.calls.lock().push((keys.to_vec(), version));
			Ok(())
		}
	}

	fn view_partition(n: u64) -> EncodedKey {
		PartitionKey::encoded(ObjectId::View(ViewId(1)), Partition(u128::from(n)))
	}

	fn tick_of(pending: Pending, reply: TickCommitReply) -> CommitterMessage {
		CommitterMessage::Tick {
			flow_id: FlowId(1),
			members: Vec::new(),
			source: SourceVersion(1),
			pending,
			owned_queues: Arc::default(),
			reply,
		}
	}

	fn counting_reads(engine: &StandardEngine, keys: &[EncodedKey]) -> Arc<CountingReads> {
		let hook = Arc::new(CountingReads {
			keys: keys.iter().cloned().collect(),
			reads: AtomicUsize::new(0),
		});
		engine.multi().store().set_testing_hooks(hook.clone());
		hook
	}

	fn send_tick(handle: &CommitterHandle, pending: Pending) -> Result<(), ValueError> {
		let (done, finished) = mpsc::channel();
		let reply: TickCommitReply =
			Box::new(move |result| done.send(result).expect("the test waits for the tick"));
		assert!(handle.actor_ref().send(tick_of(pending, reply)).is_ok(), "send tick");
		finished.recv_timeout(StdDuration::from_secs(10)).expect("the tick must reply")
	}

	fn synthetic_key(index: u64) -> EncodedKey {
		// a view partition key must stay ignored by row consumers, otherwise these tests would count lifted
		// rows
		PartitionKey::new(ObjectId::View(ViewId(1)), Partition(u128::from(index))).encode()
	}

	fn synthetic_slice(index: u64) -> FlowSlice {
		let mut combined = Pending::new();
		combined.insert(vec![synthetic_key(index)], vec![EncodedBytes(CowVec::new(vec![index as u8; 4]))]);
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(index), checkpoint_at(100 + index))];
		slice
	}

	fn build_committer_actor(engine: &StandardEngine) -> (CommitterHandle, Committer) {
		let tracker = FlowPositionTracker::new();
		let committer = Committer::new(
			engine.clone(),
			tracker.clone(),
			FlowMaterialization::new(ReaderPosition::new(), FlowPositionTracker::new()),
			engine.operator_state(),
		);
		let handle =
			engine.spawner().spawn_flow("commit-test-committer", CommitterActor::new(committer.clone()));
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
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, committer) = build_committer_actor(&engine);

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
	fn a_committed_slice_writes_its_checkpoint_to_the_operator_store() {
		// the checkpoint has one home now, and a second copy in the multi store would drift from the state it
		// is supposed to pair with
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, committer) = build_committer_actor(&engine);

		let replies = SliceReplies::new(2);
		send_slices(&handle, &replies, 2);
		replies.wait();

		for flow in 1..=2u64 {
			assert_eq!(
				committer.operators.checkpoint_get(FlowId(flow)).unwrap(),
				Some(checkpoint_at(100 + flow)),
				"the checkpoint must instead land in the operator store, where the flush writes it in \
				 the same transaction as the state it belongs to"
			);
		}
	}

	fn queue_engine() -> (TestEngine, StandardEngine, QueueId) {
		let te = TestEngine::builder().with_commit_queue().build();
		te.admin("CREATE NAMESPACE test");
		te.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
		let engine = te.inner().clone();
		let catalog = engine.catalog();
		let mut query = engine.begin_query(IdentityId::system()).unwrap();
		let mut txn = Transaction::Query(&mut query);
		let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
		let queue = catalog.find_queue_by_name(&mut txn, namespace.id(), "jobs").unwrap().unwrap().id;
		drop(txn);
		(te, engine, queue)
	}

	fn queue_items(queue: QueueId, rows: RangeInclusive<u64>) -> Vec<PendingQueueItem> {
		rows.map(|row| PendingQueueItem {
			queue,
			partition: Partition(7),
			key_hash: None,
			row_number: RowNumber(row),
			encoded: EncodedBytes(CowVec::new(vec![1])),
		})
		.collect()
	}

	fn depth(engine: &StandardEngine, queue: QueueId) -> Vec<(Partition, u64)> {
		queue_partition_counters(&system_ctx(), engine.single(), queue)
			.unwrap()
			.into_iter()
			.map(|(partition, counters)| (partition, counters.depth))
			.collect()
	}

	#[test]
	fn a_committed_slice_admits_its_queue_items_through_the_plain_queue_interceptor() {
		// Without this a queue view row exists but has no scheduling state, so no worker can ever claim it.
		let (_te, engine, queue) = queue_engine();
		let (handle, _committer) = build_committer_actor(&engine);

		let mut slice = synthetic_slice(1);
		slice.combined.admit_queue_items(queue_items(queue, 1..=2));
		let replies = SliceReplies::new(1);
		let sent = handle
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply: replies.reply(0),
			})
			.is_ok();
		assert!(sent, "send slice");
		replies.wait();

		assert_eq!(depth(&engine, queue), vec![(Partition(7), 2)]);
	}

	#[test]
	fn a_committed_tick_admits_its_queue_items_too() {
		// A tick takes its own apply path, so an item carried there must not lose its scheduling state.
		let (_te, engine, queue) = queue_engine();
		let (handle, _committer) = build_committer_actor(&engine);

		let mut pending = Pending::new();
		pending.insert(vec![synthetic_key(2)], vec![EncodedBytes(CowVec::new(vec![2u8; 4]))]);
		pending.admit_queue_items(queue_items(queue, 1..=3));
		let (done, finished) = mpsc::channel();
		assert!(handle
			.actor_ref()
			.send(CommitterMessage::Tick {
				flow_id: FlowId(1),
				members: Vec::new(),
				source: SourceVersion(1),
				pending,
				owned_queues: Arc::default(),
				reply: Box::new(move |result| done.send(result).expect("the test waits for the tick")),
			})
			.is_ok());
		finished.recv_timeout(StdDuration::from_secs(10))
			.expect("the tick must reply")
			.expect("the tick commits");

		assert_eq!(depth(&engine, queue), vec![(Partition(7), 3)]);
	}

	#[test]
	fn a_slice_carrying_its_owned_queue_commits_a_row_of_that_queue() {
		// a slice must hand its owned queues to the batch, otherwise the guard refuses every queue view row
		let (_te, engine, queue) = queue_engine();
		let (handle, _committer) = build_committer_actor(&engine);

		let mut slice = synthetic_slice(1);
		slice.combined.insert(
			vec![RowKey::new(queue, RowNumber(1)).encode()],
			vec![EncodedBytes(CowVec::new(vec![1]))],
		);
		slice.owned_queues = Arc::new(BTreeSet::from([queue]));
		let replies = SliceReplies::new(1);
		let sent = handle
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply: replies.reply(0),
			})
			.is_ok();
		assert!(sent, "send slice");
		replies.wait();

		assert!(replies.versions()[0].1 > CommitVersion(0), "the slice with its own queue row must commit");
	}

	#[test]
	fn a_tick_carrying_its_owned_queue_commits_a_row_of_that_queue() {
		// a tick takes its own apply path, so it must hand its owned queues over too
		let (_te, engine, queue) = queue_engine();
		let (handle, _committer) = build_committer_actor(&engine);

		let mut pending = Pending::new();
		pending.insert(
			vec![RowKey::new(queue, RowNumber(1)).encode()],
			vec![EncodedBytes(CowVec::new(vec![1]))],
		);
		let (done, finished) = mpsc::channel();
		assert!(handle
			.actor_ref()
			.send(CommitterMessage::Tick {
				flow_id: FlowId(1),
				members: Vec::new(),
				source: SourceVersion(1),
				pending,
				owned_queues: Arc::new(BTreeSet::from([queue])),
				reply: Box::new(move |result| done.send(result).expect("the test waits for the tick")),
			})
			.is_ok());

		finished.recv_timeout(StdDuration::from_secs(10))
			.expect("the tick must reply")
			.expect("the tick with its own queue row must commit");
	}

	#[test]
	fn a_retried_slice_admits_its_queue_items_exactly_once() {
		// The failed batch never committed, so counting its items as well as the retry's would double the
		// depth.
		let (_te, engine, queue) = queue_engine();
		let attempts = Arc::new(FailFirstQueueRead {
			reads: AtomicUsize::new(0),
		});
		engine.multi().store().set_testing_hooks(attempts.clone());
		let (_handle, committer) = build_committer_actor(&engine);

		let mut first = synthetic_slice(1);
		first.combined.admit_queue_items(queue_items(queue, 1..=1));
		let mut second = synthetic_slice(2);
		second.combined.admit_queue_items(queue_items(queue, 2..=2));
		let replies = SliceReplies::new(2);
		committer.commit_slices(
			"retry-test",
			Arc::new(vec![first, second]),
			vec![(0, replies.reply(0)), (1, replies.reply(1))],
		);
		replies.wait();

		assert_eq!(replies.versions().len(), 2, "both slices must land through their retries");
		assert_eq!(attempts.reads.load(Ordering::SeqCst), 3, "one failed batch, then one retry per slice");
		assert_eq!(depth(&engine, queue), vec![(Partition(7), 2)]);
	}

	#[test]
	fn a_retried_slice_keeps_its_owned_queue() {
		// a retry must pass the owned queues again, otherwise the retried slice is refused by the guard
		let (_te, engine, queue) = queue_engine();
		let attempts = Arc::new(FailFirstQueueRead {
			reads: AtomicUsize::new(0),
		});
		engine.multi().store().set_testing_hooks(attempts.clone());
		let (_handle, committer) = build_committer_actor(&engine);

		let owned = Arc::new(BTreeSet::from([queue]));
		let mut first = synthetic_slice(1);
		first.combined.insert(
			vec![RowKey::new(queue, RowNumber(1)).encode()],
			vec![EncodedBytes(CowVec::new(vec![1]))],
		);
		first.combined.admit_queue_items(queue_items(queue, 1..=1));
		first.owned_queues = Arc::clone(&owned);
		let mut second = synthetic_slice(2);
		second.combined.insert(
			vec![RowKey::new(queue, RowNumber(2)).encode()],
			vec![EncodedBytes(CowVec::new(vec![1]))],
		);
		second.combined.admit_queue_items(queue_items(queue, 2..=2));
		second.owned_queues = owned;
		let replies = SliceReplies::new(2);
		committer.commit_slices(
			"retry-owned-test",
			Arc::new(vec![first, second]),
			vec![(0, replies.reply(0)), (1, replies.reply(1))],
		);
		replies.wait();

		assert_eq!(attempts.reads.load(Ordering::SeqCst), 3, "one failed batch, then one retry per slice");
		let versions = replies.versions();
		assert_eq!(versions.len(), 2, "both retried slices must land");
		assert!(
			versions.iter().all(|(_, version)| *version > CommitVersion(0)),
			"each retried slice with its own queue row must commit"
		);
	}

	#[test]
	fn a_slice_committing_after_its_queue_was_dropped_leaves_nothing_behind() {
		// A slice still in flight when the view drops must not recreate rows or counters no drop will ever
		// sweep.
		let (te, engine, queue) = queue_engine();
		te.admin("DROP QUEUE test::jobs");
		let (handle, _committer) = build_committer_actor(&engine);

		let mut slice = synthetic_slice(1);
		let row = RowKey::new(queue, RowNumber(1));
		let partition = PartitionKey::new(ObjectId::Queue(queue), Partition(7));
		slice.combined.insert(
			vec![row.encode(), partition.encode()],
			vec![EncodedBytes(CowVec::new(vec![1])), EncodedBytes(CowVec::new(vec![1]))],
		);
		slice.combined.admit_queue_items(queue_items(queue, 1..=1));
		let replies = SliceReplies::new(1);
		let sent = handle
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply: replies.reply(0),
			})
			.is_ok();
		assert!(sent, "send slice");
		replies.wait();
		assert_eq!(replies.versions().len(), 1, "the rest of the slice still commits");

		assert!(depth(&engine, queue).is_empty(), "no scheduling state may outlive the queue");
		let mut query = engine.begin_query(IdentityId::system()).unwrap();
		let mut txn = Transaction::Query(&mut query);
		assert!(
			txn.get(&[row.clone().into(), partition.clone().into()]).unwrap().iter().all(Option::is_none),
			"no row or partition entry may outlive the queue"
		);
		let other = TaggedKey::decode(&synthetic_key(1)).expect("the synthetic key decodes");
		assert!(txn.get(&[other]).unwrap()[0].is_some(), "the slice's other writes must still land");
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
			combined.insert(vec![key], vec![EncodedPodRow::new(&[*tag; 4]).into_bytes()]);
		}
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.checkpoints = vec![(FlowId(1), checkpoint_at(10))];
		slice
	}

	#[test]
	fn a_failed_commit_leaves_the_operator_state_untouched() {
		// Operator state is written in the completion, so a failed commit must write none of it.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, committer) = build_committer_actor(&engine);
		engine.multi().stop_commits().expect("the commit stop must take");
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
			store.state_get(operator, &[GroupStateKey::bound_unchecked(EncodedKey::new(inner.as_slice()))])
				.unwrap()
				.pop()
				.flatten(),
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
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, committer) = build_committer_actor(&engine);
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
		assert_ne!(
			replies.versions()[0].1,
			CommitVersion(0),
			"a slice with only operator state still commits a record (OP4)"
		);

		assert_eq!(
			store.state_get(op_a, &[GroupStateKey::bound_unchecked(EncodedKey::new(inner_a.as_slice()))])
				.unwrap()
				.pop()
				.flatten(),
			Some(EncodedPodRow::new(&[1; 4])),
			"the committed slice's state must be readable from the store"
		);
		assert_eq!(
			store.state_get(op_b, &[GroupStateKey::bound_unchecked(EncodedKey::new(inner_b.as_slice()))])
				.unwrap()
				.pop()
				.flatten(),
			Some(EncodedPodRow::new(&[2; 4]))
		);
	}

	fn gated_committer_actor(engine: &StandardEngine) -> (CommitterHandle, Committer, mpsc::Sender<()>) {
		let (release, gate) = mpsc::channel::<()>();
		engine.multi().store().set_testing_hooks(Arc::new(BlockFirstRead {
			key: synthetic_key(9),
			gate: Mutex::new(Some(gate)),
		}));
		let (handle, committer) = build_committer_actor(engine);
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
		let te = TestEngine::builder().with_commit_queue().build();
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
				Transaction::Query(&mut query)
					.get(&[key.clone()])
					.map(|mut found| found.pop().flatten())
					.expect("read grouped write")
					.is_some(),
				"every slice in the group must land its writes, not only the first"
			);
		}
	}

	#[test]
	fn a_group_queued_after_a_flush_still_commits() {
		// Every group must get its own flush, or slices queued after the first flush never commit.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, committer) = build_committer_actor(&engine);

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
		let te = TestEngine::builder().with_commit_queue().build();
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
		let te = TestEngine::builder().with_commit_queue().build();
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
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, committer, release) = gated_committer_actor(&engine);

		let mut bad = sourced_slice(2, 7);
		bad.combined.insert(vec![EncodedKey::new([])], vec![EncodedBytes(CowVec::new(vec![1u8; 4]))]);

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

	fn control_slice(emitted: bool) -> FlowSlice {
		let mut slice = FlowSlice::empty();
		slice.checkpoints = vec![(FlowId(1), checkpoint_at(5))];
		slice.emitted = emitted;
		slice
	}

	fn commit_one(handle: &CommitterHandle, slice: FlowSlice) -> CommitVersion {
		let replies = SliceReplies::new(1);
		send_slice(handle, slice, replies.reply(0));
		replies.wait();
		replies.versions()[0].1
	}

	#[test]
	fn a_slice_that_emitted_without_writing_records_output() {
		// No sink emits without writing today, so only this test sees the emitted half of the output rule; a
		// rule that lost it would hide output from readers once a sink announces rows it did not store.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, committer) = build_committer_actor(&engine);

		commit_one(&handle, control_slice(false));
		assert_eq!(
			committer.materialization.materialized_through(),
			CommitVersion(0),
			"a control-only slice produced no output, or the assertion below says nothing"
		);

		let version = commit_one(&handle, control_slice(true));

		assert!(version > CommitVersion(0), "the checkpoint must commit, or no version can be recorded");
		assert_eq!(committer.materialization.materialized_through(), version);
	}

	fn row(number: u64) -> EncodedKey {
		RowKey::encoded(StorageId::view(1), RowNumber(number))
	}

	fn series_row(sequence: u64) -> EncodedKey {
		SeriesRowKey {
			storage: StorageId::view(2),
			variant_tag: None,
			key: 1,
			sequence,
		}
		.encode()
	}

	fn index_entry(value: u8) -> EncodedKey {
		PartitionKey::new(ObjectId::View(ViewId(1)), Partition(u128::from(value))).encode()
	}

	fn payload(tag: u8) -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![tag; 4]))
	}

	fn tagged(encoded: &EncodedKey) -> TaggedKey {
		TaggedKey::decode(encoded).expect("a fixture key must decode")
	}

	fn seed(engine: &StandardEngine, rows: &[(EncodedKey, u8)]) {
		let mut txn = engine.begin_command(IdentityId::system()).expect("begin seed");
		for (key, tag) in rows {
			txn.set(vec![(tagged(key), payload(*tag))]).expect("seed row");
		}
		txn.commit().expect("commit seed");
	}

	fn commit_pending(te: &TestEngine, pending: &Pending) -> (Vec<Vec<EncodedKey>>, Vec<Delta>) {
		let engine = te.inner();
		let reads = Arc::new(RecordedReads {
			calls: Mutex::new(Vec::new()),
		});
		engine.multi().store().set_testing_hooks(reads.clone());
		let mut commit = engine.begin_deferred_flow_commit().expect("begin");
		apply_pending_writes(&mut commit, pending).expect("apply pending writes");
		let version = commit.commit().expect("commit").version;
		engine.multi().store().clear_testing_hooks();
		let entry: Arc<CommitEntry> = te
			.commit_entries()
			.into_iter()
			.find(|entry| entry.version.commit == version)
			.expect("the commit must reach the commit queue");
		let calls = reads
			.calls
			.lock()
			.iter()
			.map(|(keys, _)| keys.clone())
			.filter(|keys| {
				keys.iter().any(|key| matches!(KeyTag::of(key), Some(KeyTag::Row | KeyTag::SeriesRow)))
			})
			.collect();
		(calls, entry.deltas.clone())
	}

	fn insert(key: EncodedKey, tag: u8) -> Delta {
		Delta::Set {
			key: tagged(&key),
			bytes: payload(tag),
			pre: SetPre::Absent,
		}
	}

	fn update(key: EncodedKey, pre: u8, post: u8) -> Delta {
		Delta::Set {
			key: tagged(&key),
			bytes: payload(post),
			pre: SetPre::Present(payload(pre)),
		}
	}

	fn delete(key: EncodedKey, tag: u8, visible: bool) -> Delta {
		let pre = payload(tag);
		Delta::Remove {
			key: tagged(&key),
			announce: if visible {
				RemoveAnnounce::Announced {
					pre,
				}
			} else {
				RemoveAnnounce::Unobserved {
					pre,
				}
			},
		}
	}

	#[test]
	fn a_slice_reads_every_remove_pre_image_in_one_batch_and_announces_the_same_deletes() {
		// A point read per remove makes a slice commit cost grow with every evicted or retracted row, and the
		// batch must still give each delete the pre-image and visibility the per-key reads gave it.
		let te = TestEngine::builder().with_commit_queue().build();
		seed(
			te.inner(),
			&[
				(row(1), 11),
				(row(3), 13),
				(row(4), 14),
				(series_row(1), 21),
				(series_row(2), 22),
				(index_entry(101), 31),
				(index_entry(102), 32),
				(index_entry(104), 34),
			],
		);

		let mut pending = Pending::new();
		pending.insert(vec![row(5)], vec![payload(15)]);
		pending.insert(vec![index_entry(104)], vec![payload(44)]);
		pending.remove_silent(row(4));
		pending.remove(vec![row(1)]);
		pending.remove(vec![series_row(1)]);
		pending.remove(vec![index_entry(101)]);
		pending.remove(vec![row(6)]);
		pending.remove(vec![series_row(3)]);
		pending.remove_unobserved(row(3));
		pending.remove_unobserved(series_row(2));
		pending.remove_unobserved(index_entry(102));
		pending.remove_unobserved(index_entry(103));
		pending.remove_unobserved(row(7));

		let (reads, changes) = commit_pending(&te, &pending);

		assert_eq!(
			changes,
			vec![
				insert(row(5), 15),
				update(index_entry(104), 34, 44),
				delete(row(1), 11, true),
				delete(series_row(1), 21, true),
				delete(index_entry(101), 31, true),
				delete(row(3), 13, false),
				delete(series_row(2), 22, false),
				delete(index_entry(102), 32, false),
			]
		);

		let mut query = te.inner().begin_query(IdentityId::system()).expect("begin query");
		for key in [row(1), row(3), row(4), row(6), row(7), series_row(1), series_row(2), index_entry(101)] {
			assert!(
				Transaction::Query(&mut query)
					.get(&[tagged(&key)])
					.map(|mut found| found.pop().flatten())
					.expect("read removed row")
					.is_none(),
				"every remove must land, the silent one included"
			);
		}

		assert_eq!(reads.len(), 1, "every pre-image comes from one batch read");
		for (kind, tag, gets) in [
			("row", KeyTag::Row, 5),
			("series row", KeyTag::SeriesRow, 3),
			("view partition", KeyTag::Partition, 4),
		] {
			assert_eq!(
				reads[0].iter().filter(|key| KeyTag::of(key) == Some(tag)).count(),
				gets,
				"{kind}: no point read, every set and every announced or unobserved remove in the one batch"
			);
		}
	}

	#[test]
	fn a_slice_reaches_the_commit_entry_in_the_order_its_flows_wrote_it() {
		// A version's changes are published in write order, so regrouping a slice's sets and removes changes
		// the bytes every consumer reads for that version.
		let te = TestEngine::builder().with_commit_queue().build();
		seed(te.inner(), &[(row(2), 12), (series_row(1), 21)]);

		let mut pending = Pending::new();
		pending.insert(vec![row(1)], vec![payload(1)]);
		pending.remove(vec![row(2)]);
		pending.insert(vec![row(3)], vec![payload(3)]);
		pending.remove_unobserved(series_row(1));
		pending.insert(vec![index_entry(1)], vec![payload(5)]);

		let (_, changes) = commit_pending(&te, &pending);

		assert_eq!(
			changes,
			vec![
				insert(row(1), 1),
				delete(row(2), 12, true),
				insert(row(3), 3),
				delete(series_row(1), 21, false),
				insert(index_entry(1), 5),
			]
		);
	}

	struct FailingSizing(OperatorId);

	impl PersistentHooks for FailingSizing {
		fn on_measure(&self, operator: OperatorId) -> Result<(), StoreOperatorError> {
			if operator != self.0 {
				return Ok(());
			}
			Err(StoreOperatorError::Backend {
				message: "the disk went away".to_string(),
			})
		}
	}

	fn blind_slice(operator: OperatorId, inner: &GroupStateKey, index: u64) -> FlowSlice {
		// No classify call: the committer must size this write against the store.
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(inner.as_slice())
			.expect("test fixture group-state key must decode");
		let mut combined = Pending::new();
		combined.insert(
			vec![OperatorStateKey::encoded(operator, group, keyspace, suffix)],
			vec![EncodedPodRow::new(&[index as u8; 4]).into_bytes()],
		);
		combined.insert(vec![synthetic_key(index)], vec![EncodedBytes(CowVec::new(vec![index as u8; 4]))]);
		let mut slice = FlowSlice::empty();
		slice.combined = combined;
		slice.source = Some(SourceVersion(7));
		slice.checkpoints = vec![(FlowId(index), checkpoint_at(100 + index))];
		slice
	}

	#[test]
	fn a_slice_whose_state_sizing_fails_fails_alone_and_the_rest_of_its_group_commits() {
		// A sizing error must fail only its own member before the commit, never panic after the rows are
		// durable.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let failing = OperatorId(9);
		let healthy = OperatorId(10);
		let store = OperatorStore::standard(OperatorStoreConfig {
			resident: ResidentConfig {
				storage: Resident::with_limits(ResidentLimits {
					budget: ByteSize::from_bytes(1),
					..ResidentLimits::default()
				}),
				..ResidentConfig::default()
			},
			persistent: Some(OperatorPersistentConfig::opened(PersistentTier::testing(Arc::new(
				FailingSizing(failing),
			)))),
			..OperatorStoreConfig::memory(engine.spawner())
		});
		let inner = state_inner(b"k");
		let before = EncodedPodRow::new(&[0; 4]);
		store.apply_batch_with_checkpoints(
			&[
				OperatorWrite::Insert {
					operator: failing,
					key: inner.clone(),
					post: before.clone(),
				},
				OperatorWrite::Insert {
					operator: healthy,
					key: inner.clone(),
					post: before.clone(),
				},
			],
			&[],
			&[],
			Some(next_lsn()),
		)
		.unwrap();
		flush_now(store.resident());
		store.resident().evict_to_capacity();
		assert_eq!(
			store.resident().resident_entries(),
			0,
			"precondition: a buffered row is sized from the buffer and never reaches the faulting tier"
		);

		let committer = Committer::new(
			engine.clone(),
			FlowPositionTracker::new(),
			FlowMaterialization::new(ReaderPosition::new(), FlowPositionTracker::new()),
			store.clone(),
		);
		let replies = SliceReplies::new(2);
		committer.commit_slices(
			"commit-test",
			Arc::new(vec![blind_slice(failing, &inner, 1), blind_slice(healthy, &inner, 2)]),
			vec![(0, replies.reply(0)), (1, replies.reply(1))],
		);
		replies.wait();

		let mut results: Vec<(usize, bool)> =
			replies.results.lock().iter().map(|(index, result)| (*index, result.is_ok())).collect();
		results.sort();
		assert_eq!(results, vec![(0, false), (1, true)], "only the member whose sizing failed may fail");
		assert_eq!(
			store.state_get(failing, from_ref(&inner)).unwrap().pop().flatten(),
			Some(before),
			"the failed member must leave its operator state as it was"
		);
		assert_eq!(
			store.state_get(healthy, from_ref(&inner)).unwrap().pop().flatten(),
			Some(EncodedPodRow::new(&[2; 4])),
			"the committed member must apply its operator state"
		);
		assert_eq!(store.checkpoint_get(FlowId(1)).unwrap(), None, "the failed member must not checkpoint");
		assert_eq!(store.checkpoint_get(FlowId(2)).unwrap(), Some(checkpoint_at(102)));

		let mut query = engine.begin_query(IdentityId::system()).expect("begin query");
		let row = |index: u64, query: &mut _| {
			let key = TaggedKey::decode(&synthetic_key(index)).expect("the fixture key must decode");
			Transaction::Query(query)
				.get(&[key.clone()])
				.map(|mut found| found.pop().flatten())
				.expect("read slice write")
				.is_some()
		};
		assert!(!row(1, &mut query), "the failed member's rows must not be durable");
		assert!(row(2, &mut query), "the other member's rows must commit");
	}

	fn logged_engine() -> TestEngine {
		TestEngine::builder().with_commit_queue().with_wal(|wal| wal.mode(WalMode::default())).build()
	}

	fn logged_sections(engine: &StandardEngine) -> Vec<(Lsn, CommitRecord)> {
		engine.multi()
			.wal()
			.replay(Lsn::FIRST)
			.expect("the commit log replays from its first record")
			.filter_map(|replayed| {
				let replayed = replayed.expect("every appended record decodes");
				match replayed.body {
					WalRecord::Multi(record) if !record.operator.slices.is_empty() => {
						Some((replayed.lsn, record))
					}
					_ => None,
				}
			})
			.collect()
	}

	fn plain_committer(engine: &StandardEngine, store: OperatorStore) -> Committer {
		Committer::new(
			engine.clone(),
			FlowPositionTracker::new(),
			FlowMaterialization::new(ReaderPosition::new(), FlowPositionTracker::new()),
			store,
		)
	}

	fn commit_direct(committer: &Committer, slice: FlowSlice) -> Result<CommitVersion, ValueError> {
		let replies = SliceReplies::new(1);
		committer.commit_slices("commit-test", Arc::new(vec![slice]), vec![(0, replies.reply(0))]);
		replies.wait();
		replies.results.lock().pop().expect("the slice must reply").1
	}

	fn state_write(
		operator: OperatorId,
		inner: &GroupStateKey,
		tag: u8,
	) -> (OperatorId, GroupStateKey, OperatorStateWrite) {
		(operator, inner.clone(), OperatorStateWrite::Set(EncodedPodRow::new(&[tag; 4])))
	}

	#[test]
	fn a_slice_commit_logs_its_writes_and_checkpoints_in_one_record() {
		// Replay rebuilds state and checkpoint from one record, so a checkpoint missing from it resumes the
		// flow from a stale position.
		let te = logged_engine();
		let engine = te.inner().clone();
		let committer = plain_committer(&engine, engine.operator_state());
		let operator = OperatorId(3);
		let inner = state_inner(b"a");
		let mut slice = state_slice(&[(operator, &inner, 1)]);
		slice.combined.insert(vec![synthetic_key(1)], vec![EncodedBytes(CowVec::new(vec![1u8; 4]))]);

		let version = commit_direct(&committer, slice).expect("the slice commits");

		let records = logged_sections(&engine);
		assert_eq!(records.len(), 1, "exactly one record carries the slice's operator section: {records:?}");
		let (_, record) = &records[0];
		assert_eq!(record.version, version, "the section rides in the record of the slice's own commit");
		assert!(!record.deltas.is_empty(), "the view rows must be in the same record as the section");
		assert_eq!(
			record.operator.slices,
			vec![OperatorSlice {
				writes: vec![state_write(operator, &inner, 1)],
				checkpoints: vec![(FlowId(1), checkpoint_at(10))],
				deletes: vec![],
			}]
		);
	}

	#[test]
	fn a_checkpoint_behind_the_current_one_never_reaches_the_log() {
		// A refused checkpoint that is already logged replays as durable state the store rejected live.
		let te = logged_engine();
		let engine = te.inner().clone();
		let committer = plain_committer(&engine, engine.operator_state());
		let inner = state_inner(b"a");
		commit_direct(&committer, state_slice(&[(OperatorId(3), &inner, 1)])).expect("the first slice commits");

		let mut behind = synthetic_slice(5);
		behind.checkpoints = vec![(FlowId(1), checkpoint_at(5))];
		let result = commit_direct(&committer, behind);

		assert!(result.is_err(), "a checkpoint behind the stored one must fail its slice");
		let logged: Vec<CommitVersion> = logged_sections(&engine)
			.into_iter()
			.flat_map(|(_, record)| record.operator.slices)
			.flat_map(|slice| slice.checkpoints)
			.map(|(_, checkpoint)| checkpoint.version)
			.collect();
		assert_eq!(logged, vec![CommitVersion(10)], "only the accepted checkpoint may be in the log");
		assert_eq!(committer.operators.checkpoint_get(FlowId(1)).unwrap(), Some(checkpoint_at(10)));
	}

	#[test]
	fn a_tick_commit_logs_its_operator_writes() {
		// A tick whose state stays out of the log loses or double-applies that state after a crash.
		let te = logged_engine();
		let engine = te.inner().clone();
		let (handle, _committer) = build_committer_actor(&engine);
		let operator = OperatorId(4);
		let inner = state_inner(b"t");
		let mut pending = state_slice(&[(operator, &inner, 7)]).combined;
		pending.insert(vec![synthetic_key(2)], vec![EncodedBytes(CowVec::new(vec![2u8; 4]))]);

		let (done, finished) = mpsc::channel();
		assert!(handle
			.actor_ref()
			.send(CommitterMessage::Tick {
				flow_id: FlowId(1),
				members: Vec::new(),
				source: SourceVersion(1),
				pending,
				owned_queues: Arc::default(),
				reply: Box::new(move |result| done.send(result).expect("the test waits for the tick")),
			})
			.is_ok());
		finished.recv_timeout(StdDuration::from_secs(10))
			.expect("the tick must reply")
			.expect("the tick commits");

		let records = logged_sections(&engine);
		assert_eq!(records.len(), 1, "the tick's operator writes must be in its record: {records:?}");
		assert_eq!(
			records[0].1.operator.slices,
			vec![OperatorSlice {
				writes: vec![state_write(operator, &inner, 7)],
				checkpoints: vec![],
				deletes: vec![],
			}]
		);
	}

	#[derive(Default)]
	struct RecordingFloor {
		saved: Mutex<Vec<Lsn>>,
	}

	impl WalFloorCheckpoint for RecordingFloor {
		fn make_durable(&self, _through: Lsn) -> Result<(), ValueError> {
			Ok(())
		}

		fn saved_through(&self, through: Lsn) -> Result<(), ValueError> {
			self.saved.lock().push(through);
			Ok(())
		}

		fn durable_head(&self) -> Result<Option<Lsn>, ValueError> {
			Ok(None)
		}

		fn appended_head(&self) -> Result<Option<Lsn>, ValueError> {
			Ok(None)
		}
	}

	#[test]
	fn the_operator_store_is_applied_with_the_lsn_of_the_record() {
		// A marker saved at any lsn but the record's own replays that record twice or skips one after a crash.
		let te = logged_engine();
		let engine = te.inner().clone();
		let floor = Arc::new(RecordingFloor::default());
		let (storage, _guard) = PersistentTier::sqlite_in_memory();
		let store = OperatorStore::standard(OperatorStoreConfig::sqlite(
			OperatorPersistentConfig::opened_with_checkpoint(storage, floor.clone()),
			engine.spawner(),
		));
		let committer = plain_committer(&engine, store.clone());

		commit_direct(&committer, state_slice(&[(OperatorId(5), &state_inner(b"m"), 3)]))
			.expect("the slice commits");
		flush_now(store.resident());

		let records = logged_sections(&engine);
		assert_eq!(records.len(), 1, "one record carries the slice: {records:?}");
		assert_eq!(*floor.saved.lock(), vec![records[0].0], "the flush must save exactly the record's lsn");
	}

	struct HeadAtGuard {
		wal: CommitWal,
		heads: Mutex<Vec<Option<Lsn>>>,
	}

	impl WalFloorCheckpoint for HeadAtGuard {
		fn make_durable(&self, _through: Lsn) -> Result<(), ValueError> {
			Ok(())
		}

		fn saved_through(&self, _through: Lsn) -> Result<(), ValueError> {
			Ok(())
		}

		fn durable_head(&self) -> Result<Option<Lsn>, ValueError> {
			Ok(None)
		}

		fn appended_head(&self) -> Result<Option<Lsn>, ValueError> {
			// Called exactly when a commit takes its operator guard, so it records what the log held then.
			let head = self.wal.cursors().expect("the commit log cursors read").appended;
			self.heads.lock().push(head);
			Ok(head)
		}
	}

	#[test]
	fn the_operator_guard_is_taken_before_the_section_reaches_the_log() {
		// A guard taken after the append lets an idle pass mark past a logged section that is not applied yet.
		let te = logged_engine();
		let engine = te.inner().clone();
		let floor = Arc::new(HeadAtGuard {
			wal: engine.multi().wal().clone(),
			heads: Mutex::new(Vec::new()),
		});
		let (storage, _guard) = PersistentTier::sqlite_in_memory();
		let store = OperatorStore::standard(OperatorStoreConfig::sqlite(
			OperatorPersistentConfig::opened_with_checkpoint(storage, floor.clone()),
			engine.spawner(),
		));
		let committer = plain_committer(&engine, store);

		commit_direct(&committer, state_slice(&[(OperatorId(5), &state_inner(b"g"), 3)]))
			.expect("the slice commits");

		let records = logged_sections(&engine);
		assert_eq!(records.len(), 1, "one record carries the slice: {records:?}");
		let heads = floor.heads.lock().clone();
		assert_eq!(heads.len(), 1, "one commit takes exactly one operator guard: {heads:?}");
		assert!(
			heads[0].is_none_or(|head| head < records[0].0),
			"the guard saw appended head {:?}, at or past its own section at lsn {:?}",
			heads[0],
			records[0].0
		);
	}

	#[test]
	fn a_slice_group_commit_reads_the_store_once_for_its_pre_images() {
		// A read per slice or per layer makes a group commit cost one store round trip per member.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		seed(&engine, &[(view_partition(1), 1), (view_partition(2), 2)]);
		let (_handle, committer) = build_committer_actor(&engine);
		let reads = counting_reads(
			&engine,
			&[view_partition(1), view_partition(2), view_partition(3), view_partition(4)],
		);

		let mut first = FlowSlice::empty();
		first.source = Some(SourceVersion(1));
		first.combined.insert(vec![view_partition(3)], vec![payload(3)]);
		first.combined.remove(vec![view_partition(1)]);
		let mut second = FlowSlice::empty();
		second.source = Some(SourceVersion(1));
		second.combined.insert(vec![view_partition(4)], vec![payload(4)]);
		second.combined.remove_unobserved(view_partition(2));
		let replies = SliceReplies::new(2);
		committer.commit_slices(
			"commit-test",
			Arc::new(vec![first, second]),
			vec![(0, replies.reply(0)), (1, replies.reply(1))],
		);
		replies.wait();
		let observed = reads.reads.load(Ordering::SeqCst);
		engine.multi().store().clear_testing_hooks();

		let versions = replies.versions();
		assert_eq!(versions.len(), 2, "both slices must commit");
		assert_eq!(versions[0].1, versions[1].1, "one group commits in one version");
		assert_eq!(observed, 1, "the group reads every pre-image of its keys in one store read");
	}

	#[test]
	fn a_slice_with_two_layers_commits_the_same_deltas_and_layers_as_before() {
		// Lost layer marks merge two source versions into one, so a downstream flow reads the wrong history.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, _committer) = build_committer_actor(&engine);

		let mut early = Pending::new();
		early.insert(vec![view_partition(1)], vec![payload(1)]);
		let mut late = Pending::new();
		late.insert(vec![view_partition(1)], vec![payload(2)]);
		let mut slice = FlowSlice::empty();
		slice.combined.insert(vec![view_partition(1)], vec![payload(2)]);
		slice.source = Some(SourceVersion(20));
		slice.layers = vec![(SourceVersion(10), early), (SourceVersion(20), late)];
		let version = commit_one(&handle, slice);

		let entry: Arc<CommitEntry> = te
			.commit_entries()
			.into_iter()
			.find(|entry| entry.version.commit == version)
			.expect("the commit must reach the commit queue");
		assert_eq!(entry.deltas, vec![insert(view_partition(1), 1), update(view_partition(1), 1, 2)]);
		assert_eq!(entry.layers, vec![(SourceVersion(10), 0..1), (SourceVersion(20), 1..2)]);
	}

	#[test]
	fn a_remove_only_tick_commits_silent_removes_without_reading_the_store() {
		// A hand-off of silent removes that reads the store pays a round trip for an old value nobody sees.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		seed(&engine, &[(view_partition(1), 1), (view_partition(2), 2)]);
		let (handle, _committer) = build_committer_actor(&engine);
		let reads = counting_reads(&engine, &[view_partition(1), view_partition(2)]);

		let mut pending = Pending::new();
		pending.remove_silent(view_partition(1));
		pending.remove_silent(view_partition(2));
		send_tick(&handle, pending).expect("the hand-off commits");
		let observed = reads.reads.load(Ordering::SeqCst);
		engine.multi().store().clear_testing_hooks();

		assert_eq!(observed, 0, "a silent remove never reads its old value");
		let mut query = engine.begin_query(IdentityId::system()).expect("begin query");
		for key in [view_partition(1), view_partition(2)] {
			assert!(
				Transaction::Query(&mut query)
					.get(&[tagged(&key)])
					.map(|mut found| found.pop().flatten())
					.expect("read removed row")
					.is_none(),
				"every silent remove must land"
			);
		}
	}

	#[test]
	fn a_remove_only_tick_of_keys_already_gone_commits_and_announces_nothing() {
		// A hand-off can race a key that is already gone; failing it would poison the flow for a no-op.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		let (handle, _committer) = build_committer_actor(&engine);
		let reads = counting_reads(&engine, &[view_partition(1), view_partition(2)]);

		let mut pending = Pending::new();
		pending.remove(vec![view_partition(1), view_partition(2)]);
		let result = send_tick(&handle, pending);
		let observed = reads.reads.load(Ordering::SeqCst);
		engine.multi().store().clear_testing_hooks();

		assert!(result.is_ok(), "a remove of a missing key must not fail the commit: {result:?}");
		assert_eq!(observed, 1, "the announced removes read their old values in one batch");
		let announced = te.commit_entries().into_iter().any(|entry| {
			entry.deltas.iter().any(|delta| {
				matches!(
					delta,
					Delta::Remove {
						key,
						announce: RemoveAnnounce::Announced { .. },
					} if *key == tagged(&view_partition(1)) || *key == tagged(&view_partition(2))
				)
			})
		});
		assert!(!announced, "a remove of a key nobody stored must stay silent");
	}

	#[test]
	fn a_set_after_a_remove_only_commit_carries_no_old_value() {
		// A batch read at a stale version would hand the removed bytes over as the old value of the new row.
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();
		seed(&engine, &[(view_partition(1), 1)]);
		let (handle, _committer) = build_committer_actor(&engine);

		let mut pending = Pending::new();
		pending.remove(vec![view_partition(1)]);
		send_tick(&handle, pending).expect("the remove-only tick commits");
		let mut slice = FlowSlice::empty();
		slice.combined.insert(vec![view_partition(1)], vec![payload(2)]);
		let version = commit_one(&handle, slice);

		let entry: Arc<CommitEntry> = te
			.commit_entries()
			.into_iter()
			.find(|entry| entry.version.commit == version)
			.expect("the commit must reach the commit queue");
		assert_eq!(entry.deltas, vec![insert(view_partition(1), 2)]);
	}
}
