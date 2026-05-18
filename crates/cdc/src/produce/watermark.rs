// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::common::CommitVersion;

#[derive(Clone, Default)]
pub struct CdcProducerWatermark {
	inner: Arc<AtomicU64>,
}

impl CdcProducerWatermark {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn advance(&self, version: CommitVersion) {
		self.inner.fetch_max(version.0, Ordering::SeqCst);
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.inner.load(Ordering::SeqCst))
	}
}
