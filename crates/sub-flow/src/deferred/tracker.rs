// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{common::CommitVersion, interface::catalog::shape::ShapeId};
use reifydb_runtime::sync::rwlock::RwLock;

/// Tracks the latest CDC version for each primitive (table/view/flow).
///
/// This is used to compute flow lag by comparing a flow's current version
/// to the latest version where its sources had changes.
pub struct ShapeVersionTracker {
	versions: Arc<RwLock<BTreeMap<ShapeId, CommitVersion>>>,
}

impl ShapeVersionTracker {
	pub fn new() -> Self {
		Self {
			versions: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}

	/// Update the latest version for a primitive.
	pub fn update(&self, object_id: ShapeId, version: CommitVersion) {
		let mut versions = self.versions.write();
		versions.entry(object_id)
			.and_modify(|v| {
				if version.0 > v.0 {
					*v = version;
				}
			})
			.or_insert(version);
	}

	/// Get all tracked primitive versions.
	pub fn all(&self) -> BTreeMap<ShapeId, CommitVersion> {
		let versions = self.versions.read();
		versions.clone()
	}
}

impl Default for ShapeVersionTracker {
	fn default() -> Self {
		Self::new()
	}
}
