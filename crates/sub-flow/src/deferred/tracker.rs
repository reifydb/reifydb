// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{common::CommitVersion, interface::catalog::shape::ShapeId};
use reifydb_runtime::sync::rwlock::RwLock;

pub struct ShapeVersionTracker {
	versions: Arc<RwLock<BTreeMap<ShapeId, CommitVersion>>>,
}

impl ShapeVersionTracker {
	pub fn new() -> Self {
		Self {
			versions: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}

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
