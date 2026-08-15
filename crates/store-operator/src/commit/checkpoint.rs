// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};

use crate::commit::OperatorCommitBuffer;

impl OperatorCommitBuffer {
	pub fn record_checkpoint_set(&self, flow: FlowId, version: CommitVersion) {
		self.shared.inner.lock().live.checkpoints.insert(flow, Some(version));
	}

	pub fn record_checkpoint_delete(&self, flow: FlowId) {
		self.shared.inner.lock().live.checkpoints.insert(flow, None);
	}

	pub fn lookup_checkpoint(&self, flow: FlowId) -> Option<Option<CommitVersion>> {
		let inner = self.shared.inner.lock();
		if let Some(entry) = inner.live.checkpoints.get(&flow) {
			return Some(*entry);
		}
		inner.in_flight.as_ref().and_then(|batch| batch.checkpoints.get(&flow).copied())
	}

	pub fn checkpoint_entries(&self) -> Vec<(FlowId, Option<CommitVersion>)> {
		let inner = self.shared.inner.lock();
		let mut merged: BTreeMap<FlowId, Option<CommitVersion>> = BTreeMap::new();
		if let Some(batch) = inner.in_flight.as_ref() {
			merged.extend(batch.checkpoints.iter().map(|(flow, entry)| (*flow, *entry)));
		}
		merged.extend(inner.live.checkpoints.iter().map(|(flow, entry)| (*flow, *entry)));
		merged.into_iter().collect()
	}

	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		let inner = self.shared.inner.lock();
		let mut floor: Option<CommitVersion> = None;
		if let Some(batch) = inner.in_flight.as_ref() {
			for version in batch.checkpoints.values().flatten() {
				floor = Some(floor.map_or(*version, |current| current.min(*version)));
			}
		}
		for version in inner.live.checkpoints.values().flatten() {
			floor = Some(floor.map_or(*version, |current| current.min(*version)));
		}
		floor
	}
}
