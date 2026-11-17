// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	common::CommitVersion,
	delta::{Delta, SetPre},
	interface::{
		catalog::{
			flow::FlowId,
			id::{QueueId, ViewId},
		},
		commit::CommitEntry,
	},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DroppedView {
	pub view: ViewId,
	pub queue: Option<QueueId>,
}

pub fn extract_dropped_views(entries: &[Arc<CommitEntry>]) -> Vec<DroppedView> {
	let mut dropped: Vec<DroppedView> = Vec::new();
	for entry in entries {
		for delta in &entry.deltas {
			match delta {
				Delta::Remove {
					key: TaggedKey::View(key),
					..
				} => {
					if !dropped.iter().any(|item| item.view == key.view) {
						dropped.push(DroppedView {
							view: key.view,
							queue: None,
						});
					}
				}
				Delta::Remove {
					key: TaggedKey::ViewQueue(key),
					announce,
				} => {
					let Some(bytes) = announce.pre() else {
						continue;
					};
					let queue = CatalogStore::view_queue_id(bytes);
					match dropped.iter_mut().find(|item| item.view == key.view) {
						Some(item) => item.queue = Some(queue),
						None => dropped.push(DroppedView {
							view: key.view,
							queue: Some(queue),
						}),
					}
				}
				_ => {}
			}
		}
	}
	dropped
}
