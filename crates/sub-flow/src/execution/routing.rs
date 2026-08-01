// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::TimeDomain,
	interface::{
		catalog::flow::{FlowId, OperatorId},
		change::{Change, ChangeOrigin},
	},
};
use reifydb_rql::flow::flow::FlowDag;

use crate::engine::FlowEngineInner;

fn stamp_arrival_time(change: &mut Change) {
	for diff in change.diffs.iter_mut() {
		for columns in diff.columns_mut() {
			let arrival = columns.created_at().to_vec();
			columns.system.set_time(arrival);
		}
	}
}

impl FlowEngineInner {
	pub(super) fn seed_entry_nodes(
		&self,
		flow: &FlowDag,
		flow_id: FlowId,
		change: Change,
		pending: &mut HashMap<OperatorId, Vec<Change>>,
	) {
		match &change.origin {
			ChangeOrigin::Object(source) => {
				if let Some(registrations) = self.sources.get(source) {
					for (registered_flow_id, operator_id) in registrations {
						if *registered_flow_id != flow_id {
							continue;
						}
						if flow.get_operator(operator_id).is_none() {
							continue;
						}
						let mut routed = Change {
							origin: ChangeOrigin::Flow(*operator_id),
							version: change.version,
							diffs: change.diffs.clone(),
							changed_at: change.changed_at,
						};
						if flow.time_domain() == TimeDomain::Processing {
							stamp_arrival_time(&mut routed);
						}
						pending.entry(*operator_id).or_default().push(routed);
					}
				}
			}
			ChangeOrigin::Flow(operator_id) => {
				if flow.get_operator(operator_id).is_some() {
					pending.entry(*operator_id).or_default().push(change);
				}
			}
		}
	}
}
