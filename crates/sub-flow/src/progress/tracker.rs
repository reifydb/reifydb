// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	actors::flow::FlowActorMessage,
	common::CommitVersion,
	interface::catalog::{flow::FlowId, object::ObjectId},
	lifecycle::watermark::ConsumerPositions,
};
use reifydb_runtime::{actor::mailbox::ActorRef, sync::rwlock::RwLock};

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
pub struct UpstreamPosition {
	pub position: CommitVersion,
	pub last_commit: CommitVersion,
}

#[derive(Clone)]
pub struct FlowWaker {
	actor: ActorRef<FlowActorMessage>,
	pending: Arc<AtomicBool>,
}

impl FlowWaker {
	pub fn new(actor: ActorRef<FlowActorMessage>, pending: Arc<AtomicBool>) -> Self {
		Self {
			actor,
			pending,
		}
	}

	pub fn wake(&self) {
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
	upstreams: HashMap<FlowId, Arc<FlowUpstreams>>,
	readers: HashMap<FlowId, HashSet<FlowId>>,
	wakers: HashMap<FlowId, FlowWaker>,
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

	fn readers_of(&self, producer: FlowId) -> Vec<FlowWaker> {
		let Some(readers) = self.readers.get(&producer) else {
			return Vec::new();
		};
		readers.iter().filter_map(|reader| self.wakers.get(reader).cloned()).collect()
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
			progress.readers_of(flow_id)
		};
		wake(readers);
	}

	pub fn record_commit(&self, flow_id: FlowId, commit: CommitVersion) {
		self.inner.write().advance_last_commit(flow_id, commit);
	}

	pub fn upstream_position(&self, flow_id: FlowId) -> Option<UpstreamPosition> {
		let progress = self.inner.read();
		let position = *progress.positions.get(&flow_id)?;
		Some(UpstreamPosition {
			position,
			last_commit: progress.last_commits.get(&flow_id).copied().unwrap_or(CommitVersion(0)),
		})
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
		let wakers: Vec<FlowWaker> = {
			let progress = self.inner.read();
			flow_ids.into_iter().filter_map(|flow_id| progress.wakers.get(&flow_id).cloned()).collect()
		};
		wake(wakers);
	}

	pub fn remove(&self, flow_id: FlowId) {
		let mut progress = self.inner.write();
		progress.positions.remove(&flow_id);
		progress.last_commits.remove(&flow_id);
		progress.unlink_reader(flow_id);
		progress.wakers.remove(&flow_id);
	}

	pub fn all(&self) -> HashMap<FlowId, CommitVersion> {
		self.inner.read().positions.clone()
	}
}

fn wake(readers: Vec<FlowWaker>) {
	for reader in readers {
		reader.wake();
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
		context::clock::Clock,
	};

	use super::{FlowPositionTracker, FlowWaker};

	const PRODUCER: FlowId = FlowId(1);
	const READER: FlowId = FlowId(2);

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
		let pending = Arc::new(AtomicBool::new(false));
		let tracker = FlowPositionTracker::new();
		tracker.set_upstreams(READER, HashMap::from([(PRODUCER, HashSet::new())]));
		tracker.set_waker(READER, FlowWaker::new(reader.clone(), Arc::clone(&pending)));

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
