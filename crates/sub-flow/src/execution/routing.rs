// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::{
	catalog::flow::{FlowId, FlowNodeId},
	change::{Change, ChangeOrigin},
};
use reifydb_rql::flow::flow::FlowDag;

use crate::engine::FlowEngineInner;

impl FlowEngineInner {
	pub(super) fn seed_entry_nodes(
		&self,
		flow: &FlowDag,
		flow_id: FlowId,
		change: Change,
		pending: &mut HashMap<FlowNodeId, Vec<Change>>,
	) {
		match &change.origin {
			ChangeOrigin::Shape(source) => {
				if let Some(registrations) = self.sources.get(source) {
					for (registered_flow_id, node_id) in registrations {
						if *registered_flow_id != flow_id {
							continue;
						}
						if flow.get_node(node_id).is_none() {
							continue;
						}
						let routed = Change {
							origin: ChangeOrigin::Flow(*node_id),
							version: change.version,
							diffs: change.diffs.clone(),
							changed_at: change.changed_at,
						};
						pending.entry(*node_id).or_default().push(routed);
					}
				}
			}
			ChangeOrigin::Flow(node_id) => {
				if flow.get_node(node_id).is_some() {
					pending.entry(*node_id).or_default().push(change);
				}
			}
		}
	}
}
