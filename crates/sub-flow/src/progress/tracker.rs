// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet, VecDeque},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicI64, Ordering},
	},
};

use reifydb_core::{
	actors::flow::FlowActorMessage,
	common::{CommitVersion, SourceVersion},
	interface::catalog::{flow::FlowId, id::ViewId, object::ObjectId},
	lifecycle::watermark::ConsumerPositions,
};
use reifydb_flow_async::transaction::LookupVersions;
use reifydb_runtime::{actor::mailbox::ActorRef, context::clock::Clock, sync::rwlock::RwLock};
use rustc_hash::{FxHashMap, FxHashSet};

#[derive(Clone)]
pub struct ObjectVersionTracker {
	inner: Arc<ObjectVersionTrackerInner>,
}

#[derive(Default)]
struct ObjectVersionTrackerInner {
	versions: RwLock<BTreeMap<ObjectId, CommitVersion>>,
}

impl ObjectVersionTracker {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(ObjectVersionTrackerInner::default()),
		}
	}

	pub fn update(&self, object_id: ObjectId, version: CommitVersion) {
		let mut versions = self.inner.versions.write();
		versions.entry(object_id)
			.and_modify(|v| {
				if version.0 > v.0 {
					*v = version;
				}
			})
			.or_insert(version);
	}

	pub fn all(&self) -> BTreeMap<ObjectId, CommitVersion> {
		let versions = self.inner.versions.read();
		versions.clone()
	}
}

impl Default for ObjectVersionTracker {
	fn default() -> Self {
		Self::new()
	}
}

pub type FlowUpstreams = FxHashMap<FlowId, FxHashSet<ObjectId>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Completion {
	commit: CommitVersion,
	position: CommitVersion,
	source: SourceVersion,
}

const COMPLETION_HISTORY: usize = 65_536;

pub(crate) const FLOW_WAKE_COALESCE_NANOS: i64 = 20_000_000;

#[derive(Clone)]
pub struct FlowWaker {
	actor: ActorRef<FlowActorMessage>,
	pending: Arc<AtomicBool>,
	clock: Clock,
	next_wake_nanos: Arc<AtomicI64>,
}

impl FlowWaker {
	pub fn new(actor: ActorRef<FlowActorMessage>, pending: Arc<AtomicBool>, clock: Clock) -> Self {
		Self {
			actor,
			pending,
			clock,
			next_wake_nanos: Arc::new(AtomicI64::new(0)),
		}
	}

	pub fn wake(&self) {
		let now = self.clock.now().to_nanos();
		let next = self.next_wake_nanos.load(Ordering::Relaxed);
		if now < next {
			self.pending.store(true, Ordering::SeqCst);
			return;
		}
		let deadline = now.saturating_add(FLOW_WAKE_COALESCE_NANOS);
		if self.next_wake_nanos.compare_exchange(next, deadline, Ordering::SeqCst, Ordering::Relaxed).is_err() {
			return;
		}
		self.send();
	}

	pub fn wake_now(&self) {
		let deadline = self.clock.now().to_nanos().saturating_add(FLOW_WAKE_COALESCE_NANOS);
		self.next_wake_nanos.store(deadline, Ordering::SeqCst);
		self.send();
	}

	fn send(&self) {
		if self.pending.swap(true, Ordering::SeqCst) {
			return;
		}
		let _ = self.actor.send(FlowActorMessage::Wake);
	}
}

#[derive(Clone)]
pub struct FlowPositionTracker {
	inner: Arc<RwLock<FlowProgress>>,
}

#[derive(Default)]
struct FlowProgress {
	positions: HashMap<FlowId, CommitVersion>,
	last_commits: HashMap<FlowId, CommitVersion>,
	completions: HashMap<FlowId, VecDeque<Completion>>,
	upstreams: HashMap<FlowId, Arc<FlowUpstreams>>,
	readers: HashMap<FlowId, HashSet<FlowId>>,
	wakers: HashMap<FlowId, FlowWaker>,
	source_counts: HashMap<FlowId, usize>,
	sources: HashMap<FlowId, SourceVersion>,
	lookup_producers: HashMap<FlowId, BTreeMap<ViewId, FlowId>>,
}

impl FlowProgress {
	fn advance_position(&mut self, flow_id: FlowId, version: CommitVersion) -> bool {
		match self.positions.get_mut(&flow_id) {
			Some(current) if version <= *current => false,
			Some(current) => {
				*current = version;
				true
			}
			None => {
				self.positions.insert(flow_id, version);
				true
			}
		}
	}

	fn advance_last_commit(&mut self, flow_id: FlowId, version: CommitVersion) {
		let current = self.last_commits.entry(flow_id).or_insert(version);
		if version > *current {
			*current = version;
		}
	}

	fn advance_source(&mut self, flow_id: FlowId, source: SourceVersion) {
		let current = self.sources.entry(flow_id).or_insert(source);
		if source > *current {
			*current = source;
		}
	}

	fn record_landed(&mut self, flow_id: FlowId, position: CommitVersion) {
		let landed = self.last_commits.get(&flow_id).copied().unwrap_or(CommitVersion(0));
		self.record_completion(flow_id, landed, position);
		self.prune_completions(flow_id);
	}

	fn reader_floor(&self, flow_id: FlowId) -> Option<CommitVersion> {
		let readers = self.readers.get(&flow_id)?;
		readers.iter().map(|reader| self.positions.get(reader).copied().unwrap_or(CommitVersion(0))).min()
	}

	fn prune_completions(&mut self, flow_id: FlowId) {
		let Some(floor) = self.reader_floor(flow_id) else {
			return;
		};
		let Some(history) = self.completions.get_mut(&flow_id) else {
			return;
		};
		let above = history.partition_point(|entry| entry.commit <= floor);
		history.drain(..above.saturating_sub(1));
	}

	fn record_completion(&mut self, flow_id: FlowId, commit: CommitVersion, position: CommitVersion) {
		let source = self.sources.get(&flow_id).copied().unwrap_or(SourceVersion(position.0));
		let history = self.completions.entry(flow_id).or_default();
		let source = history.back().map_or(source, |last| last.source.max(source));
		match history.back_mut() {
			Some(last) if commit < last.commit => return,
			Some(last) if commit == last.commit => {
				last.position = last.position.max(position);
				return;
			}
			_ => {}
		}
		history.push_back(Completion {
			commit,
			position,
			source,
		});
		if history.len() > COMPLETION_HISTORY {
			history.pop_front();
		}
	}

	fn complete_through(&self, flow_id: FlowId, read_to: CommitVersion) -> Option<CommitVersion> {
		let history = self.completions.get(&flow_id)?;
		let above = history.partition_point(|entry| entry.commit <= read_to);
		history.get(above.checked_sub(1)?).map(|entry| entry.position)
	}

	fn commit_through_source(&self, flow_id: FlowId, source: SourceVersion) -> Option<CommitVersion> {
		let history = self.completions.get(&flow_id)?;
		let above = history.partition_point(|entry| entry.source <= source);
		history.get(above.checked_sub(1)?).map(|entry| entry.commit)
	}

	fn commit_through_commit(&self, flow_id: FlowId, commit: CommitVersion) -> Option<CommitVersion> {
		let history = self.completions.get(&flow_id)?;
		let above = history.partition_point(|entry| entry.commit <= commit);
		history.get(above.checked_sub(1)?).map(|entry| entry.commit)
	}

	fn folds(&self, flow_id: FlowId) -> bool {
		!self.readers.contains_key(&flow_id)
			&& self.source_counts.get(&flow_id).copied().unwrap_or(usize::MAX) <= 1
	}

	fn readers_of(&self, producer: FlowId) -> Vec<(FlowWaker, bool)> {
		let Some(readers) = self.readers.get(&producer) else {
			return Vec::new();
		};
		readers.iter()
			.filter_map(|reader| self.wakers.get(reader).map(|waker| (waker.clone(), self.folds(*reader))))
			.collect()
	}

	fn link_reader(&mut self, reader: FlowId, upstreams: FlowUpstreams) {
		for producer in upstreams.keys() {
			self.readers.entry(*producer).or_default().insert(reader);
		}
		self.upstreams.insert(reader, Arc::new(upstreams));
	}

	fn unlink_reader(&mut self, reader: FlowId) {
		let Some(previous) = self.upstreams.remove(&reader) else {
			return;
		};
		for producer in previous.keys() {
			if let Some(readers) = self.readers.get_mut(producer) {
				readers.remove(&reader);
				if readers.is_empty() {
					self.readers.remove(producer);
				}
			}
		}
	}
}

impl FlowPositionTracker {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(FlowProgress::default())),
		}
	}

	pub fn update(&self, flow_id: FlowId, version: CommitVersion) {
		let readers = {
			let mut progress = self.inner.write();
			if !progress.advance_position(flow_id, version) {
				return;
			}
			progress.record_landed(flow_id, version);
			progress.readers_of(flow_id)
		};
		wake(readers);
	}

	pub fn update_committed(
		&self,
		flow_id: FlowId,
		version: CommitVersion,
		commit: CommitVersion,
		source: SourceVersion,
	) {
		let readers = {
			let mut progress = self.inner.write();
			progress.advance_last_commit(flow_id, commit);
			progress.advance_source(flow_id, source);
			if !progress.advance_position(flow_id, version) {
				return;
			}
			progress.record_landed(flow_id, version);
			progress.readers_of(flow_id)
		};
		wake(readers);
	}

	pub fn record_commit(&self, flow_id: FlowId, commit: CommitVersion) {
		self.inner.write().advance_last_commit(flow_id, commit);
	}

	pub fn upstream_complete_through(&self, flow_id: FlowId, read_to: CommitVersion) -> Option<CommitVersion> {
		self.inner.read().complete_through(flow_id, read_to)
	}

	pub fn commit_through_source(&self, flow_id: FlowId, source: SourceVersion) -> Option<CommitVersion> {
		self.inner.read().commit_through_source(flow_id, source)
	}

	pub fn commit_through_commit(&self, flow_id: FlowId, commit: CommitVersion) -> Option<CommitVersion> {
		self.inner.read().commit_through_commit(flow_id, commit)
	}

	pub fn set_lookup_producers(&self, flow_id: FlowId, producers: BTreeMap<ViewId, FlowId>) {
		let mut progress = self.inner.write();
		if producers.is_empty() {
			progress.lookup_producers.remove(&flow_id);
		} else {
			progress.lookup_producers.insert(flow_id, producers);
		}
	}

	pub fn lookup_versions(&self, flow_id: FlowId) -> FlowLookupVersions {
		FlowLookupVersions {
			tracker: self.clone(),
			flow_id,
		}
	}

	pub fn set_upstreams(&self, flow_id: FlowId, upstreams: FlowUpstreams) {
		let mut progress = self.inner.write();
		progress.unlink_reader(flow_id);
		progress.link_reader(flow_id, upstreams);
	}

	pub fn upstreams(&self, flow_id: FlowId) -> Arc<FlowUpstreams> {
		self.inner.read().upstreams.get(&flow_id).cloned().unwrap_or_default()
	}

	pub fn has_readers(&self, flow_id: FlowId) -> bool {
		self.inner.read().readers.contains_key(&flow_id)
	}

	pub fn set_waker(&self, flow_id: FlowId, waker: FlowWaker) {
		self.inner.write().wakers.insert(flow_id, waker);
	}

	pub fn wake_flows(&self, flow_ids: impl IntoIterator<Item = FlowId>) {
		wake(self.wakers_of(flow_ids));
	}

	pub fn wake_flows_now(&self, flow_ids: impl IntoIterator<Item = FlowId>) {
		for (waker, _) in self.wakers_of(flow_ids) {
			waker.wake_now();
		}
	}

	pub fn set_source_count(&self, flow_id: FlowId, count: usize) {
		self.inner.write().source_counts.insert(flow_id, count);
	}

	pub fn registered_flows(&self) -> Vec<FlowId> {
		let mut flows: Vec<FlowId> = self.inner.read().source_counts.keys().copied().collect();
		flows.sort();
		flows
	}

	fn wakers_of(&self, flow_ids: impl IntoIterator<Item = FlowId>) -> Vec<(FlowWaker, bool)> {
		let progress = self.inner.read();
		flow_ids.into_iter()
			.filter_map(|flow_id| {
				progress.wakers.get(&flow_id).map(|waker| (waker.clone(), progress.folds(flow_id)))
			})
			.collect()
	}

	pub fn remove(&self, flow_id: FlowId) {
		let mut progress = self.inner.write();
		progress.positions.remove(&flow_id);
		progress.last_commits.remove(&flow_id);
		progress.unlink_reader(flow_id);
		progress.wakers.remove(&flow_id);
		progress.source_counts.remove(&flow_id);
		progress.sources.remove(&flow_id);
		progress.lookup_producers.remove(&flow_id);
	}

	pub fn all(&self) -> HashMap<FlowId, CommitVersion> {
		self.inner.read().positions.clone()
	}
}

pub struct FlowLookupVersions {
	tracker: FlowPositionTracker,
	flow_id: FlowId,
}

impl LookupVersions for FlowLookupVersions {
	fn view_version(&self, view: ViewId, source: SourceVersion) -> Option<CommitVersion> {
		let producer = *self.tracker.inner.read().lookup_producers.get(&self.flow_id)?.get(&view)?;
		self.tracker.commit_through_source(producer, source)
	}

	fn view_commit_through(&self, view: ViewId, commit: CommitVersion) -> Option<CommitVersion> {
		let producer = *self.tracker.inner.read().lookup_producers.get(&self.flow_id)?.get(&view)?;
		self.tracker.commit_through_commit(producer, commit)
	}
}

fn wake(readers: Vec<(FlowWaker, bool)>) {
	for (reader, folds) in readers {
		if folds {
			reader.wake();
		} else {
			reader.wake_now();
		}
	}
}

impl ConsumerPositions for FlowPositionTracker {
	fn min_position(&self) -> Option<CommitVersion> {
		self.inner.read().positions.values().copied().min()
	}
}

impl Default for FlowPositionTracker {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		mem::forget,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
			mpsc,
		},
	};

	use reifydb_core::{
		actors::flow::FlowActorMessage,
		common::{CommitVersion, SourceVersion},
		interface::catalog::flow::FlowId,
	};
	use reifydb_runtime::{
		actor::{
			context::Context,
			mailbox::ActorRef,
			system::{ActorConfig, ActorSystem},
			traits::{Actor, Directive},
		},
		context::clock::{Clock, MockClock},
	};
	use reifydb_value::value::duration::Duration;
	use rustc_hash::{FxHashMap, FxHashSet};

	use super::{COMPLETION_HISTORY, FlowPositionTracker, FlowProgress, FlowWaker};

	const PRODUCER: FlowId = FlowId(1);
	const READER: FlowId = FlowId(2);

	fn cv(version: u64) -> CommitVersion {
		CommitVersion(version)
	}

	#[test]
	fn a_producer_position_is_not_trusted_until_its_commit_is_read() {
		let mut progress = FlowProgress::default();
		progress.record_completion(PRODUCER, cv(12), cv(10));

		assert_eq!(
			progress.complete_through(PRODUCER, cv(11)),
			None,
			"the commit carrying the output for position 10 is not read yet, so nothing may pass"
		);
		assert_eq!(progress.complete_through(PRODUCER, cv(12)), Some(cv(10)));
	}

	#[test]
	fn a_reader_behind_the_newest_commit_still_resolves_an_older_position() {
		let mut progress = FlowProgress::default();
		progress.record_completion(PRODUCER, cv(10), cv(4));
		progress.record_completion(PRODUCER, cv(20), cv(9));
		progress.record_completion(PRODUCER, cv(30), cv(15));

		assert_eq!(
			progress.complete_through(PRODUCER, cv(25)),
			Some(cv(9)),
			"a reader must get the newest position it has fully read, or an active producer freezes it \
			 forever at its cursor"
		);
		assert_eq!(progress.complete_through(PRODUCER, cv(20)), Some(cv(9)), "an exact commit must resolve");
		assert_eq!(
			progress.complete_through(PRODUCER, cv(9)),
			None,
			"below every recorded commit nothing is proven, and claiming a position would skip output"
		);
	}

	#[test]
	fn pruning_keeps_the_entry_the_slowest_reader_still_needs() {
		let mut progress = FlowProgress::default();
		progress.link_reader(READER, FxHashMap::from_iter([(PRODUCER, FxHashSet::default())]));
		progress.advance_position(READER, cv(25));
		for version in 1..=5u64 {
			progress.advance_last_commit(PRODUCER, cv(version * 10));
			progress.record_landed(PRODUCER, cv(version * 10));
		}

		assert_eq!(
			progress.complete_through(PRODUCER, cv(25)),
			Some(cv(20)),
			"the entry answering the slowest reader must survive, or pruning recreates the freeze it \
			 exists to prevent"
		);
		let oldest = progress.completions.get(&PRODUCER).and_then(|h| h.front()).map(|e| e.commit.0);
		assert_eq!(oldest, Some(20), "everything strictly below that entry can never be asked for again");
	}

	#[test]
	fn a_reader_that_never_advances_holds_the_whole_history() {
		let mut progress = FlowProgress::default();
		progress.link_reader(READER, FxHashMap::from_iter([(PRODUCER, FxHashSet::default())]));
		progress.advance_position(READER, cv(0));
		for version in 1..=40u64 {
			progress.advance_last_commit(PRODUCER, cv(version * 10));
			progress.record_landed(PRODUCER, cv(version * 10));
		}

		assert_eq!(
			progress.completions.get(&PRODUCER).map(|h| h.len()),
			Some(40),
			"a stuck reader must keep its answers reachable, or it can never resolve and never recover"
		);
	}

	#[test]
	fn an_unknown_producer_resolves_to_nothing() {
		let progress = FlowProgress::default();
		assert_eq!(
			progress.complete_through(PRODUCER, cv(100)),
			None,
			"a producer that has never committed proves nothing about what it will emit"
		);
	}

	#[test]
	fn completion_history_keeps_the_newest_pairs_within_its_bound() {
		let mut progress = FlowProgress::default();
		for version in 1..=(COMPLETION_HISTORY as u64 + 10) {
			progress.record_completion(PRODUCER, cv(version * 2), cv(version));
		}

		assert_eq!(
			progress.completions.get(&PRODUCER).map(|h| h.len()),
			Some(COMPLETION_HISTORY),
			"an unbounded history would grow with uptime"
		);
		assert_eq!(
			progress.complete_through(PRODUCER, cv(20)),
			None,
			"a reader that falls off the retained window must not be handed a position it cannot prove"
		);
	}

	#[test]
	fn a_repeated_or_regressing_commit_does_not_extend_the_history() {
		let mut progress = FlowProgress::default();
		progress.record_completion(PRODUCER, cv(20), cv(9));
		progress.record_completion(PRODUCER, cv(20), cv(11));
		progress.record_completion(PRODUCER, cv(15), cv(12));

		assert_eq!(
			progress.completions.get(&PRODUCER).map(|h| h.len()),
			Some(1),
			"the lookup scans for the largest commit at or below a bound, so the history must stay sorted"
		);
		assert_eq!(
			progress.complete_through(PRODUCER, cv(20)),
			Some(cv(11)),
			"consuming further without emitting anything new is still progress the reader may use"
		);
		assert_eq!(
			progress.complete_through(PRODUCER, cv(19)),
			None,
			"a commit that went backwards must not lower the bound a reader already resolved"
		);
	}

	#[test]
	fn a_checkpoint_carrying_no_output_records_at_the_commit_that_last_landed() {
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(34), cv(22), SourceVersion(34));
		tracker.update_committed(PRODUCER, cv(38), cv(0), SourceVersion(38));

		assert_eq!(
			tracker.upstream_complete_through(PRODUCER, cv(38)),
			Some(cv(38)),
			"an empty slice commits at version none, and taking that literally drops the advance and \
			 pins every reader at the last version that carried rows"
		);
	}

	#[test]
	fn a_position_published_without_a_commit_still_reaches_readers() {
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(4), cv(10), SourceVersion(4));
		tracker.update(PRODUCER, cv(7));

		assert_eq!(
			tracker.upstream_complete_through(PRODUCER, cv(10)),
			Some(cv(7)),
			"a producer that consumed further and emitted nothing has nothing left to land, so a reader \
			 at its last commit must not be held back"
		);
	}

	struct WakeRecorder {
		received: mpsc::Sender<&'static str>,
	}

	impl Actor for WakeRecorder {
		type State = ();
		type Message = FlowActorMessage;

		fn init(&self, _ctx: &Context<Self::Message>) {}

		fn handle(&self, _state: &mut (), msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
			let kind = match msg {
				FlowActorMessage::Wake => "wake",
				FlowActorMessage::Sample => "sample",
				FlowActorMessage::Tick => "tick",
				_ => "other",
			};
			self.received.send(kind).expect("the test still listens for flow messages");
			Directive::Continue
		}

		fn config(&self) -> ActorConfig {
			ActorConfig::new()
		}
	}

	fn next_message(received: &mpsc::Receiver<&'static str>) -> &'static str {
		received.recv_timeout(Duration::from_seconds_const(10).to_std())
			.expect("the recorder must receive the next message")
	}

	fn send_marker(reader: &ActorRef<FlowActorMessage>, marker: FlowActorMessage) {
		assert!(reader.send(marker).is_ok(), "send marker");
	}

	#[test]
	fn upstream_steps_to_a_reader_with_a_pending_wake_merge_into_one_message() {
		// An upstream step must not queue a second wake on a reader already woken, or the flow pool floods.
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let (sender, received) = mpsc::channel();
		let reader = actor_system
			.spawner()
			.spawn_flow(
				"wake-recorder",
				WakeRecorder {
					received: sender,
				},
			)
			.actor_ref()
			.clone();
		let pending = Arc::new(AtomicBool::new(false));
		let tracker = FlowPositionTracker::new();
		tracker.set_upstreams(READER, FxHashMap::from_iter([(PRODUCER, FxHashSet::default())]));
		tracker.set_waker(READER, FlowWaker::new(reader.clone(), Arc::clone(&pending), clock));

		tracker.update(PRODUCER, CommitVersion(1));
		tracker.update(PRODUCER, CommitVersion(2));
		tracker.update_committed(PRODUCER, CommitVersion(3), CommitVersion(3), SourceVersion(3));
		send_marker(&reader, FlowActorMessage::Sample);
		assert_eq!(next_message(&received), "wake", "the first upstream step must wake the reader");
		assert_eq!(
			next_message(&received),
			"sample",
			"upstream steps before the reader drains must merge into the first wake"
		);

		pending.store(false, Ordering::SeqCst);
		tracker.update_committed(PRODUCER, CommitVersion(4), CommitVersion(4), SourceVersion(4));
		send_marker(&reader, FlowActorMessage::Tick);
		assert_eq!(
			next_message(&received),
			"wake",
			"a step after the reader cleared its wake must wake it again, or it never sees that step"
		);
		assert_eq!(next_message(&received), "tick", "exactly one wake must follow the cleared flag");
	}

	fn coalescing_harness(
		source_count: usize,
	) -> (Clock, FlowPositionTracker, ActorRef<FlowActorMessage>, Arc<AtomicBool>, mpsc::Receiver<&'static str>)
	{
		let clock = Clock::Mock(MockClock::from_millis(0));
		let actor_system = ActorSystem::testing(Clock::testing());
		let (sender, received) = mpsc::channel();
		let reader = actor_system
			.spawner()
			.spawn_flow(
				"wake-recorder",
				WakeRecorder {
					received: sender,
				},
			)
			.actor_ref()
			.clone();
		forget(actor_system);
		let pending = Arc::new(AtomicBool::new(false));
		let tracker = FlowPositionTracker::new();
		tracker.set_upstreams(READER, FxHashMap::from_iter([(PRODUCER, FxHashSet::default())]));
		tracker.set_source_count(READER, source_count);
		tracker.set_waker(READER, FlowWaker::new(reader.clone(), Arc::clone(&pending), clock.clone()));
		(clock, tracker, reader, pending, received)
	}

	#[test]
	fn a_reader_that_folds_drops_upstream_wakes_inside_the_coalesce_window() {
		// A folding reader woken on every producer commit never accumulates a backlog worth folding, so
		// wakes inside the window must be dropped rather than queued.
		let (clock, tracker, reader, pending, received) = coalescing_harness(1);

		tracker.update(PRODUCER, CommitVersion(1));
		assert_eq!(next_message(&received), "wake", "the first upstream step must wake a folding reader");

		pending.store(false, Ordering::SeqCst);
		tracker.update(PRODUCER, CommitVersion(2));
		send_marker(&reader, FlowActorMessage::Sample);
		assert_eq!(
			next_message(&received),
			"sample",
			"a step inside the coalesce window must be dropped, or the reader never folds"
		);

		pending.store(false, Ordering::SeqCst);
		clock.as_mock().expect("the harness drives a mock clock").advance_millis(21);
		tracker.update(PRODUCER, CommitVersion(3));
		send_marker(&reader, FlowActorMessage::Tick);
		assert_eq!(
			next_message(&received),
			"wake",
			"a step after the window elapses must wake the reader, or it stalls until the full wake"
		);
		assert_eq!(next_message(&received), "tick", "exactly one wake must follow the elapsed window");
	}

	#[test]
	fn a_reader_that_cuts_per_source_is_woken_on_every_upstream_step() {
		// Delaying a reader that cuts one source version per step buys no folding, so it must never be
		// coalesced or it pays latency for nothing.
		let (_clock, tracker, reader, pending, received) = coalescing_harness(2);

		tracker.update(PRODUCER, CommitVersion(1));
		assert_eq!(next_message(&received), "wake", "the first upstream step must wake a cutting reader");

		pending.store(false, Ordering::SeqCst);
		tracker.update(PRODUCER, CommitVersion(2));
		send_marker(&reader, FlowActorMessage::Sample);
		assert_eq!(
			next_message(&received),
			"wake",
			"a cutting reader must be woken inside the window too, or it lags for no gain"
		);
		assert_eq!(next_message(&received), "sample", "exactly one wake must follow the second step");
	}

	#[test]
	fn the_full_wake_ignores_the_coalesce_window() {
		// The periodic full wake is the liveness backstop that makes dropping a wake safe, so it must never
		// itself be dropped.
		let (_clock, tracker, reader, pending, received) = coalescing_harness(1);

		tracker.update(PRODUCER, CommitVersion(1));
		assert_eq!(next_message(&received), "wake", "the first upstream step must wake a folding reader");

		pending.store(false, Ordering::SeqCst);
		tracker.wake_flows_now([READER]);
		send_marker(&reader, FlowActorMessage::Sample);
		assert_eq!(
			next_message(&received),
			"wake",
			"the full wake must reach a reader inside its coalesce window, or liveness rests on nothing"
		);
		assert_eq!(next_message(&received), "sample", "exactly one wake must follow the full wake");
	}

	#[test]
	fn a_flow_has_readers_only_while_another_flow_lists_it_upstream() {
		// The slice cut and the committer split both key off this flag, so a stale true caps a terminal flow
		// at one commit per source version and a stale false lets a read producer fold out of order.
		let tracker = FlowPositionTracker::new();
		assert!(!tracker.has_readers(PRODUCER), "an unlinked flow must have no readers");

		tracker.set_upstreams(READER, FxHashMap::from_iter([(PRODUCER, FxHashSet::default())]));
		assert!(tracker.has_readers(PRODUCER), "a linked producer must have readers");
		assert!(!tracker.has_readers(READER), "the reader itself is read by nobody");

		tracker.set_upstreams(READER, FxHashMap::default());
		assert!(!tracker.has_readers(PRODUCER), "relinking the reader without the producer must clear it");

		tracker.set_upstreams(READER, FxHashMap::from_iter([(PRODUCER, FxHashSet::default())]));
		tracker.remove(READER);
		assert!(!tracker.has_readers(PRODUCER), "removing the last reader must clear it");
	}
}

#[cfg(test)]
mod lookup_tests {
	use std::collections::BTreeMap;

	use reifydb_core::{
		common::{CommitVersion, SourceVersion},
		interface::catalog::{flow::FlowId, id::ViewId},
	};
	use reifydb_flow_async::transaction::LookupVersions;
	use rustc_hash::{FxHashMap, FxHashSet};

	use super::FlowPositionTracker;

	const PRODUCER: FlowId = FlowId(1);
	const READER: FlowId = FlowId(2);
	const PRICES: ViewId = ViewId(7);

	fn cv(version: u64) -> CommitVersion {
		CommitVersion(version)
	}

	fn sv(version: u64) -> SourceVersion {
		SourceVersion(version)
	}

	fn sources(tracker: &FlowPositionTracker, flow: FlowId) -> Vec<u64> {
		tracker.inner
			.read()
			.completions
			.get(&flow)
			.map(|h| h.iter().map(|e| e.source.0).collect())
			.unwrap_or_default()
	}

	#[test]
	fn commit_through_source_answers_by_source_not_by_position() {
		// Keyed by position, source 14 would read C20 and miss the source-12 rows committed at C30.
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(5), cv(10), sv(5));
		tracker.update_committed(PRODUCER, cv(8), cv(20), sv(8));
		tracker.update_committed(PRODUCER, cv(15), cv(30), sv(12));

		assert_eq!(tracker.commit_through_source(PRODUCER, sv(5)), Some(cv(10)));
		assert_eq!(tracker.commit_through_source(PRODUCER, sv(9)), Some(cv(20)));
		assert_eq!(
			tracker.commit_through_source(PRODUCER, sv(14)),
			Some(cv(30)),
			"source 12 landed at C30, so a lookup at source 14 must read C30"
		);
		assert_eq!(
			tracker.commit_through_source(PRODUCER, sv(4)),
			None,
			"no commit covers source 4, so the lookup must not be handed a version"
		);
	}

	#[test]
	fn commit_through_commit_answers_the_last_commit_at_or_below_a_version() {
		// A lookup below its lease floor may read at the floor only if this finds no commit after its own.
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(5), cv(10), sv(5));
		tracker.update_committed(PRODUCER, cv(8), cv(20), sv(8));

		assert_eq!(tracker.commit_through_commit(PRODUCER, cv(19)), Some(cv(10)));
		assert_eq!(tracker.commit_through_commit(PRODUCER, cv(20)), Some(cv(20)));
		assert_eq!(tracker.commit_through_commit(PRODUCER, cv(9)), None, "no commit at or below 9");
	}

	#[test]
	fn a_tick_between_two_sourced_commits_never_lowers_a_completion_source() {
		// A tick must keep the last slice's source, otherwise the history unsorts and source lookups miss rows.
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(10), cv(20), sv(10));
		tracker.record_commit(PRODUCER, cv(25));
		tracker.update(PRODUCER, cv(12));
		tracker.update_committed(PRODUCER, cv(15), cv(30), sv(15));

		let recorded = sources(&tracker, PRODUCER);
		assert_eq!(recorded, vec![10, 10, 15], "each landing must keep or raise the source");
		assert!(recorded.is_sorted(), "sources must never decrease across a tick");
		assert_eq!(
			tracker.commit_through_source(PRODUCER, sv(12)),
			Some(cv(25)),
			"the tick commit holds everything through source 10 plus the tick's own output"
		);
		assert_eq!(tracker.commit_through_source(PRODUCER, sv(15)), Some(cv(30)));
		assert_eq!(tracker.commit_through_source(PRODUCER, sv(9)), None);
	}

	#[test]
	fn a_lower_source_after_a_higher_one_is_clamped_not_recorded() {
		// A lower stamp after a higher one must never unsort the history, otherwise the answer misses rows.
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(10), cv(20), sv(10));
		tracker.update_committed(PRODUCER, cv(11), cv(30), sv(7));

		assert_eq!(sources(&tracker, PRODUCER), vec![10, 10]);
		assert_eq!(tracker.commit_through_source(PRODUCER, sv(10)), Some(cv(30)));
	}

	#[test]
	fn an_equal_commit_keeps_its_first_source() {
		// An empty slice must keep the commit's first source, otherwise a lookup between the two sources finds
		// no commit.
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(10), cv(20), sv(10));
		tracker.update_committed(PRODUCER, cv(14), cv(0), sv(14));

		assert_eq!(sources(&tracker, PRODUCER), vec![10]);
		assert_eq!(tracker.commit_through_source(PRODUCER, sv(12)), Some(cv(20)));
		assert_eq!(tracker.commit_through_source(PRODUCER, sv(14)), Some(cv(20)));
	}

	#[test]
	fn lookup_versions_resolve_the_view_through_its_producer() {
		// The operator only knows the view; without the producer map it cannot find whose commits to read.
		let tracker = FlowPositionTracker::new();
		tracker.update_committed(PRODUCER, cv(8), cv(20), sv(8));
		tracker.set_lookup_producers(READER, BTreeMap::from([(PRICES, PRODUCER)]));
		let versions = tracker.lookup_versions(READER);

		assert_eq!(versions.view_version(PRICES, sv(9)), Some(cv(20)));
		assert_eq!(versions.view_version(ViewId(8), sv(9)), None, "an unmapped view has no producer");
		assert_eq!(
			tracker.lookup_versions(PRODUCER).view_version(PRICES, sv(9)),
			None,
			"the map is per reading flow"
		);

		tracker.remove(READER);
		assert_eq!(versions.view_version(PRICES, sv(9)), None, "a removed reader keeps no producer map");
	}

	#[test]
	fn pruning_for_a_lookup_reader_keeps_the_entry_its_next_source_needs() {
		// Pruning must keep the newest entry at or below the reader, otherwise IP2 fires on a version that
		// exists.
		let tracker = FlowPositionTracker::new();
		tracker.set_upstreams(READER, FxHashMap::from_iter([(PRODUCER, FxHashSet::default())]));
		tracker.update(READER, cv(25));
		for step in 1..=5u64 {
			tracker.update_committed(PRODUCER, cv(step * 10), cv(step * 10), sv(step * 10));
		}

		assert_eq!(tracker.commit_through_source(PRODUCER, sv(26)), Some(cv(20)));
		assert_eq!(tracker.commit_through_source(PRODUCER, sv(50)), Some(cv(50)));
	}
}
