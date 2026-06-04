// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{change::CatalogTrackFlowNodeChangeOperations, flow::FlowNode};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackFlowNodeChangeOperations for AdminTransaction {
	fn track_flow_node_created(&mut self, node: FlowNode) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(node),
			op: Create,
		};
		self.changes.add_flow_node_change(change);
		Ok(())
	}

	fn track_flow_node_deleted(&mut self, node: FlowNode) -> Result<()> {
		let change = Change {
			pre: Some(node),
			post: None,
			op: Delete,
		};
		self.changes.add_flow_node_change(change);
		Ok(())
	}
}
