// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::{
	catalog::flow::{FlowId, OperatorId},
	change::{Change, ChangeOrigin},
};
use reifydb_rql::flow::flow::FlowDag;

use crate::{engine::FlowEngineInner, transaction::interface::FlowTransaction};

impl<T: FlowTransaction> FlowEngineInner<T> {
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
						let routed = Change {
							origin: ChangeOrigin::Flow(*operator_id),
							version: change.version,
							diffs: change.diffs.clone(),
							changed_at: change.changed_at,
						};
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
