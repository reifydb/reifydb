// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::common::CommitVersion;

#[derive(Clone, Default)]
pub struct ControlFrontier {
	inner: Arc<AtomicU64>,
}

impl ControlFrontier {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn store(&self, version: CommitVersion) {
		self.inner.fetch_max(version.0, Ordering::AcqRel);
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.inner.load(Ordering::Acquire))
	}
}
