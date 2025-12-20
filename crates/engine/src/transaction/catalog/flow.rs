// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperationType::{Create, Update};
use reifydb_catalog::transaction::CatalogTrackFlowChangeOperations;
use reifydb_core::interface::{
	Change, FlowDef, FlowId, NamespaceId, OperationType, OperationType::Delete, TransactionalFlowChanges,
};
use reifydb_type::IntoFragment;

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl CatalogTrackFlowChangeOperations for StandardCommandTransaction {
	fn track_flow_def_created(&mut self, flow: FlowDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: None,
			post: Some(flow),
			op: Create,
		};
		self.changes.add_flow_def_change(change);
		Ok(())
	}

	fn track_flow_def_updated(&mut self, pre: FlowDef, post: FlowDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_flow_def_change(change);
		Ok(())
	}

	fn track_flow_def_deleted(&mut self, flow: FlowDef) -> reifydb_core::Result<()> {
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
		// Find the last change for this flow ID
		for change in self.changes.flow_def.iter().rev() {
			if let Some(flow) = &change.post {
				if flow.id == id {
					return Some(flow);
				}
			}
			if let Some(flow) = &change.pre {
				if flow.id == id && change.op == Delete {
					// Flow was deleted
					return None;
				}
			}
		}
		None
	}

	fn find_flow_by_name<'a>(&self, namespace: NamespaceId, name: impl IntoFragment<'a>) -> Option<&FlowDef> {
		let name = name.into_fragment();
		// Find the last change for this flow name
		for change in self.changes.flow_def.iter().rev() {
			if let Some(flow) = &change.post {
				if flow.namespace == namespace && flow.name == name.text() {
					return Some(flow);
				}
			}
			if let Some(flow) = &change.pre {
				if flow.namespace == namespace && flow.name == name.text() && change.op == Delete {
					// Flow was deleted
					return None;
				}
			}
		}
		None
	}

	fn is_flow_deleted(&self, id: FlowId) -> bool {
		// Check if this flow was deleted in this transaction
		self.changes
			.flow_def
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|f| f.id == id).unwrap_or(false))
	}

	fn is_flow_deleted_by_name<'a>(&self, namespace: NamespaceId, name: impl IntoFragment<'a>) -> bool {
		let name = name.into_fragment();
		// Check if this flow was deleted in this transaction
		self.changes.flow_def.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|f| f.namespace == namespace && f.name == name.text())
					.unwrap_or(false)
		})
	}
}

impl TransactionalFlowChanges for StandardQueryTransaction {
	fn find_flow(&self, _id: FlowId) -> Option<&FlowDef> {
		None
	}

	fn find_flow_by_name<'a>(&self, _namespace: NamespaceId, _name: impl IntoFragment<'a>) -> Option<&FlowDef> {
		None
	}

	fn is_flow_deleted(&self, _id: FlowId) -> bool {
		false
	}

	fn is_flow_deleted_by_name<'a>(&self, _namespace: NamespaceId, _name: impl IntoFragment<'a>) -> bool {
		false
	}
}
