// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowId,
		cdc::{Cdc, SystemChange},
	},
	key::{Key, kind::KeyKind},
};

pub fn extract_new_flows(cdcs: &[Cdc]) -> Vec<(FlowId, CommitVersion)> {
	let mut flows = Vec::new();
	for cdc in cdcs {
		for change in &cdc.system_changes {
			if let Some(kind) = Key::kind(change.key())
				&& kind == KeyKind::Flow && let SystemChange::Insert {
				key,
				..
			} = change && let Some(Key::Flow(flow_key)) = Key::decode(key)
			{
				flows.push((flow_key.flow, cdc.version));
			}
		}
	}
	flows
}

pub fn extract_new_flow_ids(cdcs: &[Cdc]) -> Vec<FlowId> {
	extract_new_flows(cdcs).into_iter().map(|(id, _)| id).collect()
}

pub fn extract_deleted_flow_ids(cdcs: &[Cdc]) -> Vec<FlowId> {
	let mut flow_ids = Vec::new();
	for cdc in cdcs {
		for change in &cdc.system_changes {
			if let Some(kind) = Key::kind(change.key())
				&& kind == KeyKind::Flow && let SystemChange::Delete {
				key,
				..
			} = change && let Some(Key::Flow(flow_key)) = Key::decode(key)
			{
				flow_ids.push(flow_key.flow);
			}
		}
	}
	flow_ids
}
