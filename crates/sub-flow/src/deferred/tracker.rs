// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{flow::FlowId, object::ObjectId},
	lifecycle::watermark::ConsumerPositions,
};
use reifydb_runtime::sync::rwlock::RwLock;

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

#[derive(Clone)]
pub struct FlowPositionTracker {
	inner: Arc<FlowPositionTrackerInner>,
}

#[derive(Default)]
struct FlowPositionTrackerInner {
	positions: RwLock<BTreeMap<FlowId, CommitVersion>>,
}

impl FlowPositionTracker {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(FlowPositionTrackerInner::default()),
		}
	}

	pub fn update(&self, flow_id: FlowId, version: CommitVersion) {
		let mut positions = self.inner.positions.write();
		positions
			.entry(flow_id)
			.and_modify(|v| {
				if version.0 > v.0 {
					*v = version;
				}
			})
			.or_insert(version);
	}

	pub fn remove(&self, flow_id: FlowId) {
		self.inner.positions.write().remove(&flow_id);
	}

	pub fn all(&self) -> BTreeMap<FlowId, CommitVersion> {
		let positions = self.inner.positions.read();
		positions.clone()
	}
}

impl ConsumerPositions for FlowPositionTracker {
	fn min_position(&self) -> Option<CommitVersion> {
		self.inner.positions.read().values().copied().min()
	}
}

impl Default for FlowPositionTracker {
	fn default() -> Self {
		Self::new()
	}
}
