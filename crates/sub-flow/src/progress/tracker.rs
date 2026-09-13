// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::Arc,
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
pub struct FlowPositionTracker {
	inner: Arc<RwLock<FlowProgress>>,
}

#[derive(Default)]
struct FlowProgress {
	positions: HashMap<FlowId, CommitVersion>,
	last_commits: HashMap<FlowId, CommitVersion>,
	upstreams: HashMap<FlowId, Arc<FlowUpstreams>>,
	readers: HashMap<FlowId, HashSet<FlowId>>,
	wakers: HashMap<FlowId, ActorRef<FlowActorMessage>>,
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

	fn readers_of(&self, producer: FlowId) -> Vec<ActorRef<FlowActorMessage>> {
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

	pub fn set_waker(&self, flow_id: FlowId, waker: ActorRef<FlowActorMessage>) {
		self.inner.write().wakers.insert(flow_id, waker);
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

fn wake(readers: Vec<ActorRef<FlowActorMessage>>) {
	for reader in readers {
		let _ = reader.send(FlowActorMessage::Wake);
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
