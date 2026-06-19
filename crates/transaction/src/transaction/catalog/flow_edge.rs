// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{change::CatalogTrackFlowEdgeChangeOperations, flow::FlowEdge};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackFlowEdgeChangeOperations for AdminTransaction {
	fn track_flow_edge_created(&mut self, edge: FlowEdge) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(edge),
			op: Create,
		};
		self.changes.add_flow_edge_change(change);
		Ok(())
	}

	fn track_flow_edge_deleted(&mut self, edge: FlowEdge) -> Result<()> {
		let change = Change {
			pre: Some(edge),
			post: None,
			op: Delete,
		};
		self.changes.add_flow_edge_change(change);
		Ok(())
	}
}
