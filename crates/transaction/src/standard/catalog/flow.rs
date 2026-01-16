// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackFlowChangeOperations,
	flow::{FlowDef, FlowId},
	id::NamespaceId,
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalFlowChanges,
	},
	standard::StandardCommandTransaction,
};

impl CatalogTrackFlowChangeOperations for StandardCommandTransaction {
	fn track_flow_def_created(&mut self, flow: FlowDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(flow),
			op: Create,
		};
		self.changes.add_flow_def_change(change);
		Ok(())
	}

	fn track_flow_def_updated(&mut self, pre: FlowDef, post: FlowDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_flow_def_change(change);
		Ok(())
	}

	fn track_flow_def_deleted(&mut self, flow: FlowDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(flow),
			post: None,
			op: Delete,
		};
		self.changes.add_flow_def_change(change);
		Ok(())
	}
}

impl TransactionalFlowChanges for StandardCommandTransaction {
	fn find_flow(&self, id: FlowId) -> Option<&FlowDef> {
		for change in self.changes.flow_def.iter().rev() {
			if let Some(flow) = &change.post {
				if flow.id == id {
					return Some(flow);
				}
			}
			if let Some(flow) = &change.pre {
				if flow.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&FlowDef> {
		for change in self.changes.flow_def.iter().rev() {
			if let Some(flow) = &change.post {
				if flow.namespace == namespace && flow.name == name {
					return Some(flow);
				}
			}
			if let Some(flow) = &change.pre {
				if flow.namespace == namespace && flow.name == name && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_flow_deleted(&self, id: FlowId) -> bool {
		self.changes
			.flow_def
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|f| f.id == id).unwrap_or(false))
	}

	fn is_flow_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.flow_def.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|f| f.namespace == namespace && f.name == name)
					.unwrap_or(false)
		})
	}
}
