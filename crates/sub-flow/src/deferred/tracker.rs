// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{flow::FlowId, shape::ShapeId},
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::reifydb_assertions;

#[derive(Clone)]
pub struct ShapeVersionTracker {
	inner: Arc<ShapeVersionTrackerInner>,
}

#[derive(Default)]
struct ShapeVersionTrackerInner {
	versions: RwLock<BTreeMap<ShapeId, CommitVersion>>,
}

impl ShapeVersionTracker {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(ShapeVersionTrackerInner::default()),
		}
	}

	pub fn update(&self, object_id: ShapeId, version: CommitVersion) {
		let mut versions = self.inner.versions.write();
		versions.entry(object_id)
			.and_modify(|v| {
				reifydb_assertions! {
					let prev = v.0;
					let new = version.0;
					assert!(
						new >= prev,
						"shape version moved backwards for shape {:?}: a monotonic tracker must never decrease (prev={prev} new={new})",
						object_id
					);
				}
				if version.0 > v.0 {
					*v = version;
				}
			})
			.or_insert(version);
	}

	pub fn all(&self) -> BTreeMap<ShapeId, CommitVersion> {
		let versions = self.inner.versions.read();
		versions.clone()
	}
}

impl Default for ShapeVersionTracker {
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

	pub fn all(&self) -> BTreeMap<FlowId, CommitVersion> {
		let positions = self.inner.positions.read();
		positions.clone()
	}
}

impl Default for FlowPositionTracker {
	fn default() -> Self {
		Self::new()
	}
}
