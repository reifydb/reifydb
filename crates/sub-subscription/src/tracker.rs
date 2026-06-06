// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::SubscriptionId, shape::ShapeId},
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::reifydb_assertions;

#[derive(Clone)]
pub struct SubscriptionSourceTracker {
	inner: Arc<SubscriptionSourceTrackerInner>,
}

#[derive(Default)]
struct SubscriptionSourceTrackerInner {
	versions: RwLock<BTreeMap<ShapeId, CommitVersion>>,
}

impl SubscriptionSourceTracker {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(SubscriptionSourceTrackerInner::default()),
		}
	}

	pub fn update(&self, shape_id: ShapeId, version: CommitVersion) {
		let mut versions = self.inner.versions.write();
		versions
			.entry(shape_id)
			.and_modify(|v| {
				reifydb_assertions! {
					let prev = v.0;
					let new = version.0;
					assert!(
						new >= prev,
						"source shape version moved backwards for shape {:?}: a monotonic tracker must never decrease (prev={prev} new={new})",
						shape_id
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

impl Default for SubscriptionSourceTracker {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Clone)]
pub struct SubscriptionPositionTracker {
	inner: Arc<SubscriptionPositionTrackerInner>,
}

#[derive(Default)]
struct SubscriptionPositionTrackerInner {
	positions: RwLock<BTreeMap<SubscriptionId, CommitVersion>>,
}

impl SubscriptionPositionTracker {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(SubscriptionPositionTrackerInner::default()),
		}
	}

	pub fn update(&self, subscription_id: SubscriptionId, version: CommitVersion) {
		let mut positions = self.inner.positions.write();
		positions
			.entry(subscription_id)
			.and_modify(|v| {
				if version.0 > v.0 {
					*v = version;
				}
			})
			.or_insert(version);
	}

	pub fn remove(&self, subscription_id: &SubscriptionId) {
		self.inner.positions.write().remove(subscription_id);
	}

	pub fn all(&self) -> BTreeMap<SubscriptionId, CommitVersion> {
		let positions = self.inner.positions.read();
		positions.clone()
	}
}

impl Default for SubscriptionPositionTracker {
	fn default() -> Self {
		Self::new()
	}
}
