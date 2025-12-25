// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Tracks the latest CDC version where each primitive had changes.

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{CommitVersion, interface::PrimitiveId};
use tokio::sync::RwLock;

/// Tracks the latest CDC version where each primitive had changes.
///
/// This is used to compute flow lag - the difference between the primitive's
/// latest change version and the flow's processed version.
pub struct PrimitiveVersionTracker {
	/// Map of primitive_id -> latest version with changes
	versions: Arc<RwLock<HashMap<PrimitiveId, CommitVersion>>>,
}

impl PrimitiveVersionTracker {
	/// Create a new empty tracker.
	pub fn new() -> Self {
		Self {
			versions: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	/// Update the latest version for a primitive.
	///
	/// Called by the dispatcher when it sees row changes for a primitive.
	pub async fn update(&self, primitive_id: PrimitiveId, version: CommitVersion) {
		let mut versions = self.versions.write().await;
		versions.insert(primitive_id, version);
	}

	/// Get all tracked primitive versions.
	///
	/// Returns a snapshot of the current state.
	pub async fn all(&self) -> HashMap<PrimitiveId, CommitVersion> {
		self.versions.read().await.clone()
	}
}

impl Default for PrimitiveVersionTracker {
	fn default() -> Self {
		Self::new()
	}
}
