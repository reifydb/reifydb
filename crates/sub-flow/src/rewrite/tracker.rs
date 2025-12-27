// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Tracks the latest CDC version where each primitive had changes.

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{CommitVersion, interface::PrimitiveId};
use tokio::sync::RwLock;

/// Tracks the latest CDC version for each primitive (table/view/flow).
///
/// This is used to compute flow lag by comparing a flow's current version
/// to the latest version where its sources had changes.
pub struct PrimitiveVersionTracker {
	versions: Arc<RwLock<HashMap<PrimitiveId, CommitVersion>>>,
}

impl PrimitiveVersionTracker {
	pub fn new() -> Self {
		Self {
			versions: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	/// Update the latest version for a primitive.
	pub async fn update(&self, primitive_id: PrimitiveId, version: CommitVersion) {
		let mut versions = self.versions.write().await;
		versions.entry(primitive_id)
			.and_modify(|v| {
				if version.0 > v.0 {
					*v = version;
				}
			})
			.or_insert(version);
	}

	/// Get all tracked primitive versions.
	pub async fn all(&self) -> HashMap<PrimitiveId, CommitVersion> {
		let versions = self.versions.read().await;
		versions.clone()
	}
}

impl Default for PrimitiveVersionTracker {
	fn default() -> Self {
		Self::new()
	}
}
