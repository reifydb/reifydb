// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	sync::Arc,
};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::SubscriptionId, object::ObjectId},
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::reifydb_assertions;

#[derive(Clone)]
pub struct SubscriptionSourceTracker {
	inner: Arc<SubscriptionSourceTrackerInner>,
}

#[derive(Default)]
struct SubscriptionSourceTrackerInner {
	versions: RwLock<BTreeMap<ObjectId, CommitVersion>>,
}

impl SubscriptionSourceTracker {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(SubscriptionSourceTrackerInner::default()),
		}
	}

	pub fn update(&self, object_id: ObjectId, version: CommitVersion) {
		let mut versions = self.inner.versions.write();
		versions
			.entry(object_id)
			.and_modify(|v| {
				reifydb_assertions! {
					let prev = v.0;
					let new = version.0;
					assert!(
						new >= prev,
						"source shape version moved backwards for shape {:?}: a monotonic tracker must never decrease (prev={prev} new={new})",
						object_id
					);
				}
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

#[derive(Clone)]
pub struct SubscribedObjects {
	inner: Arc<RwLock<SubscribedObjectsInner>>,
}

#[derive(Default)]
struct SubscribedObjectsInner {
	by_subscription: HashMap<SubscriptionId, BTreeSet<ObjectId>>,
	counts: BTreeMap<ObjectId, usize>,
}

impl SubscribedObjects {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(SubscribedObjectsInner::default())),
		}
	}

	pub fn register(&self, subscription_id: SubscriptionId, objects: impl IntoIterator<Item = ObjectId>) {
		let objects: BTreeSet<ObjectId> = objects.into_iter().collect();
		let mut inner = self.inner.write();
		for object in &objects {
			*inner.counts.entry(*object).or_insert(0) += 1;
		}
		inner.by_subscription.insert(subscription_id, objects);
	}

	pub fn unregister(&self, subscription_id: &SubscriptionId) {
		let mut inner = self.inner.write();
		if let Some(objects) = inner.by_subscription.remove(subscription_id) {
			release(&mut inner.counts, objects);
		}
	}

	pub fn snapshot(&self) -> BTreeSet<ObjectId> {
		self.inner.read().counts.keys().copied().collect()
	}
}

impl Default for SubscribedObjects {
	fn default() -> Self {
		Self::new()
	}
}

fn release(counts: &mut BTreeMap<ObjectId, usize>, objects: BTreeSet<ObjectId>) {
	for object in objects {
		if let Some(count) = counts.get_mut(&object) {
			*count -= 1;
			if *count == 0 {
				counts.remove(&object);
			}
		}
	}
}
