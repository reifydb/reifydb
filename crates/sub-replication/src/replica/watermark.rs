// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::common::CommitVersion;

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
