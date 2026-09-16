// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet, VecDeque},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use reifydb_core::{
	actors::flow::FlowActorMessage,
	common::CommitVersion,
	interface::catalog::{flow::FlowId, object::ObjectId},
	lifecycle::watermark::ConsumerPositions,
};
use reifydb_runtime::{actor::mailbox::ActorRef, context::clock::Clock, sync::rwlock::RwLock};

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

pub type FlowUpstreams = HashMap<FlowId, HashSet<ObjectId>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Completion {
	commit: CommitVersion,
	position: CommitVersion,
}

const COMPLETION_HISTORY: usize = 16_384;

const FLOW_WAKE_COALESCE_NANOS: u64 = 20_000_000;

#[derive(Clone)]
pub struct FlowWaker {
	actor: ActorRef<FlowActorMessage>,
	pending: Arc<AtomicBool>,
	clock: Clock,
	next_wake_nanos: Arc<AtomicU64>,
}

impl FlowWaker {
	pub fn new(actor: ActorRef<FlowActorMessage>, pending: Arc<AtomicBool>, clock: Clock) -> Self {
		Self {
			actor,
			pending,
			clock,
			next_wake_nanos: Arc::new(AtomicU64::new(0)),
		}
	}

	pub fn wake(&self) {
		let now = self.clock.now().to_nanos();
		let next = self.next_wake_nanos.load(Ordering::Relaxed);
		if now < next {
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

	fn record_landed(&mut self, flow_id: FlowId, position: CommitVersion) {
		let landed = self.last_commits.get(&flow_id).copied().unwrap_or(CommitVersion(0));
		self.record_completion(flow_id, landed, position);
	}

	fn record_completion(&mut self, flow_id: FlowId, commit: CommitVersion, position: CommitVersion) {
		let history = self.completions.entry(flow_id).or_default();
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

	pub fn update_committed(&self, flow_id: FlowId, version: CommitVersion, commit: CommitVersion) {
		let readers = {
			let mut progress = self.inner.write();
			progress.advance_last_commit(flow_id, commit);
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
	}

	pub fn all(&self) -> HashMap<FlowId, CommitVersion> {
		self.inner.read().positions.clone()
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
		collections::{HashMap, HashSet},
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
			mpsc,
		},
		time::Duration,
	};

	use reifydb_core::{actors::flow::FlowActorMessage, common::CommitVersion, interface::catalog::flow::FlowId};
	use reifydb_runtime::{
		actor::{
			context::Context,
			mailbox::ActorRef,
			system::{ActorConfig, ActorSystem},
			traits::{Actor, Directive},
		},
		context::clock::{Clock, MockClock},
	};

	use super::{FlowPositionTracker, FlowProgress, FlowWaker};

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
		for version in 1..=(super::COMPLETION_HISTORY as u64 + 10) {
			progress.record_completion(PRODUCER, cv(version * 2), cv(version));
		}

		assert_eq!(
			progress.completions.get(&PRODUCER).map(|h| h.len()),
			Some(super::COMPLETION_HISTORY),
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
		tracker.update_committed(PRODUCER, cv(34), cv(22));
		tracker.update_committed(PRODUCER, cv(38), cv(0));

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
		tracker.update_committed(PRODUCER, cv(4), cv(10));
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
		received.recv_timeout(Duration::from_secs(10)).expect("the recorder must receive the next message")
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
		tracker.set_upstreams(READER, HashMap::from([(PRODUCER, HashSet::new())]));
		tracker.set_waker(READER, FlowWaker::new(reader.clone(), Arc::clone(&pending), clock));

		tracker.update(PRODUCER, CommitVersion(1));
		tracker.update(PRODUCER, CommitVersion(2));
		tracker.update_committed(PRODUCER, CommitVersion(3), CommitVersion(3));
		send_marker(&reader, FlowActorMessage::Sample);
		assert_eq!(next_message(&received), "wake", "the first upstream step must wake the reader");
		assert_eq!(
			next_message(&received),
			"sample",
			"upstream steps before the reader drains must merge into the first wake"
		);

		pending.store(false, Ordering::SeqCst);
		tracker.update_committed(PRODUCER, CommitVersion(4), CommitVersion(4));
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
	) -> (Clock, FlowPositionTracker, ActorRef<FlowActorMessage>, Arc<AtomicBool>, mpsc::Receiver<&'static str>) {
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
		std::mem::forget(actor_system);
		let pending = Arc::new(AtomicBool::new(false));
		let tracker = FlowPositionTracker::new();
		tracker.set_upstreams(READER, HashMap::from([(PRODUCER, HashSet::new())]));
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

		tracker.set_upstreams(READER, HashMap::from([(PRODUCER, HashSet::new())]));
		assert!(tracker.has_readers(PRODUCER), "a linked producer must have readers");
		assert!(!tracker.has_readers(READER), "the reader itself is read by nobody");

		tracker.set_upstreams(READER, HashMap::new());
		assert!(!tracker.has_readers(PRODUCER), "relinking the reader without the producer must clear it");

		tracker.set_upstreams(READER, HashMap::from([(PRODUCER, HashSet::new())]));
		tracker.remove(READER);
		assert!(!tracker.has_readers(PRODUCER), "removing the last reader must clear it");
	}
}
