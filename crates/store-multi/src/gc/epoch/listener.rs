// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::event::{EventListener, transaction::PostCommitEvent};
use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};

pub struct VersionEpochListener {
	epoch: VersionEpoch,
	clock: Clock,
}

impl VersionEpochListener {
	pub fn new(epoch: VersionEpoch, clock: Clock) -> Self {
		Self {
			epoch,
			clock,
		}
	}
}

impl EventListener<PostCommitEvent> for VersionEpochListener {
	fn on(&self, event: &PostCommitEvent) {
		self.epoch.record(self.clock.now_nanos(), event.version().0);
	}
}
