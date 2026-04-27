// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::common::CommitVersion;

/// Monotonic watermark of "highest CDC-producer-processed commit version".
///
/// Once `get() >= V`, every `PostCommitEvent` for versions `<= V` has been
/// fully handled by the producer actor, so the cdc table contains the
/// complete set of CDC entries for those versions (with holes only for
/// system-only commits that produced no entry). The compactor reads this
/// watermark to ensure it never packs a block that could have a "hole"
/// later filled in by an in-flight producer write.
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
