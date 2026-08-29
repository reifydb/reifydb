// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
};

use crate::tier::resident::OperatorResidentState;

impl OperatorResidentState {
	pub fn record_checkpoint_set(&self, flow: FlowId, version: CommitVersion) {
		self.shared().global.lock().checkpoints.insert(flow, Some(version));
	}

	pub fn record_checkpoint_delete(&self, flow: FlowId) {
		self.shared().global.lock().checkpoints.insert(flow, None);
	}

	pub fn lookup_checkpoint(&self, flow: FlowId) -> Option<Option<CommitVersion>> {
		let global = self.shared().global.lock();
		if let Some(entry) = global.checkpoints.get(&flow) {
			return Some(*entry);
		}
		global.in_flight_checkpoints.get(&flow).copied()
	}

	pub fn checkpoint_entries(&self) -> Vec<(FlowId, Option<CommitVersion>)> {
		let global = self.shared().global.lock();
		let mut merged: BTreeMap<FlowId, Option<CommitVersion>> = BTreeMap::new();
		merged.extend(global.in_flight_checkpoints.iter().map(|(flow, entry)| (*flow, *entry)));
		merged.extend(global.checkpoints.iter().map(|(flow, entry)| (*flow, *entry)));
		merged.into_iter().collect()
	}

	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		let global = self.shared().global.lock();
		let mut floor: Option<CommitVersion> = None;
		for version in global.in_flight_checkpoints.values().flatten() {
			floor = Some(floor.map_or(*version, |current| current.min(*version)));
		}
		for version in global.checkpoints.values().flatten() {
			floor = Some(floor.map_or(*version, |current| current.min(*version)));
		}
		floor
	}

	pub fn durable_position(&self, operator: OperatorId) -> Option<CommitVersion> {
		let slot = self.shared().slot(operator)?;
		slot.inner.lock().durable_position
	}
}
