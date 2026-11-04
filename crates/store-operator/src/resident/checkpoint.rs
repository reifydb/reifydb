// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::FlowId, operator::FlowCheckpoint},
};
use reifydb_wal::lsn::Lsn;

use crate::resident::Resident;

impl Resident {
	pub fn record_checkpoint_set(&self, flow: FlowId, checkpoint: FlowCheckpoint) {
		let mut global = self.shared().global.lock();
		global.checkpoints.insert(flow, Some(checkpoint));
		global.first_unflushed.entry(flow).or_insert(checkpoint.lsn);
	}

	pub fn record_checkpoint_delete(&self, flow: FlowId) {
		let mut global = self.shared().global.lock();
		global.checkpoints.insert(flow, None);
		global.first_unflushed.remove(&flow);
	}

	pub fn lookup_checkpoint(&self, flow: FlowId) -> Option<Option<FlowCheckpoint>> {
		let global = self.shared().global.lock();
		if let Some(entry) = global.checkpoints.get(&flow) {
			return Some(*entry);
		}
		global.in_flight_checkpoints.get(&flow).copied()
	}

	pub fn checkpoint_entries(&self) -> Vec<(FlowId, Option<FlowCheckpoint>)> {
		let global = self.shared().global.lock();
		let mut merged: BTreeMap<FlowId, Option<FlowCheckpoint>> = BTreeMap::new();
		merged.extend(global.in_flight_checkpoints.iter().map(|(flow, entry)| (*flow, *entry)));
		merged.extend(global.checkpoints.iter().map(|(flow, entry)| (*flow, *entry)));
		merged.into_iter().collect()
	}

	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		let global = self.shared().global.lock();
		let mut floor: Option<CommitVersion> = None;
		for checkpoint in global.in_flight_checkpoints.values().chain(global.checkpoints.values()).flatten() {
			floor = Some(floor.map_or(checkpoint.version, |current| current.min(checkpoint.version)));
		}
		floor
	}

	pub fn checkpoint_lsn_floor(&self) -> Option<Lsn> {
		self.shared().global.lock().first_unflushed.values().min().copied()
	}
}
