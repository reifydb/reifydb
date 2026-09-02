// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowId,
		cdc::{Cdc, CdcChange},
	},
	key::{flow::FlowKey, typed::key::Key},
};

pub fn extract_new_flows(cdcs: &[Arc<Cdc>]) -> Vec<(FlowId, CommitVersion)> {
	let mut flows = Vec::new();
	for cdc in cdcs {
		for change in &cdc.changes {
			if let CdcChange::Insert {
				key,
				..
			} = change && let Some(flow_key) = FlowKey::decode(key)
			{
				flows.push((flow_key.flow, cdc.version));
			}
		}
	}
	flows
}

pub fn extract_deleted_flow_ids(cdcs: &[Arc<Cdc>]) -> Vec<FlowId> {
	let mut flow_ids = Vec::new();
	for cdc in cdcs {
		for change in &cdc.changes {
			if let CdcChange::Delete {
				key,
				..
			} = change && let Some(flow_key) = FlowKey::decode(key)
			{
				flow_ids.push(flow_key.flow);
			}
		}
	}
	flow_ids
}
