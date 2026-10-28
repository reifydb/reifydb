// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	delta::{Delta, SetPre},
	interface::{catalog::flow::FlowId, commit::CommitEntry},
	key::any::TaggedKey,
};
use reifydb_wal::lsn::Lsn;

pub fn extract_new_flows(entries: &[Arc<CommitEntry>]) -> Vec<(FlowId, CommitVersion, Lsn)> {
	let mut flows = Vec::new();
	for entry in entries {
		for delta in &entry.deltas {
			if let Delta::Set {
				key: TaggedKey::Flow(flow_key),
				pre: SetPre::Absent,
				..
			} = delta
			{
				flows.push((flow_key.flow, entry.version.commit, entry.lsn));
			}
		}
	}
	flows
}

pub fn extract_deleted_flow_ids(entries: &[Arc<CommitEntry>]) -> Vec<FlowId> {
	let mut flow_ids = Vec::new();
	for entry in entries {
		for delta in &entry.deltas {
			if let Delta::Remove {
				key: TaggedKey::Flow(flow_key),
				..
			} = delta
			{
				flow_ids.push(flow_key.flow);
			}
		}
	}
	flow_ids
}
