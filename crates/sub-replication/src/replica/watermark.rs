// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::common::CommitVersion;

/// Highest commit version that the replica applier has successfully applied.
///
/// Cloneable handle backed by an `Arc<AtomicU64>`, so the applier and any
/// observers (e.g. `db.watermarks().replica()`) share the same atomic.
#[derive(Clone, Default)]
pub struct ReplicaWatermark(Arc<AtomicU64>);

impl ReplicaWatermark {
	pub fn new() -> Self {
		Self(Arc::new(AtomicU64::new(0)))
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.0.load(Ordering::Acquire))
	}

	pub fn store(&self, v: CommitVersion) {
		self.0.store(v.0, Ordering::Release);
	}
}
