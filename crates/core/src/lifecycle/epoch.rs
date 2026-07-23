// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};

use crate::{
	common::CommitVersion,
	event::{EventListener, transaction::PostCommitEvent},
};

pub trait EpochSource: Send + Sync + 'static {
	fn now_nanos(&self) -> u64;

	fn current_version(&self) -> Option<CommitVersion>;
}

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
