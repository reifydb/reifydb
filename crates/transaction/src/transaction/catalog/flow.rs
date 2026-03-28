// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackFlowChangeOperations,
	flow::{Flow, FlowId},
	id::NamespaceId,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalFlowChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackFlowChangeOperations for AdminTransaction {
	fn track_flow_created(&mut self, flow: Flow) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(flow),
			op: Create,
		};
		self.changes.add_flow_change(change);
		Ok(())
	}

	fn track_flow_updated(&mut self, pre: Flow, post: Flow) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_flow_change(change);
		Ok(())
	}

	fn track_flow_deleted(&mut self, flow: Flow) -> Result<()> {
		let change = Change {
			pre: Some(flow),
			post: None,
			op: Delete,
		};
		self.changes.add_flow_change(change);
		Ok(())
	}
}

impl TransactionalFlowChanges for AdminTransaction {
	fn find_flow(&self, id: FlowId) -> Option<&Flow> {
		for change in self.changes.flow.iter().rev() {
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

	fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Flow> {
		for change in self.changes.flow.iter().rev() {
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
			.flow
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|f| f.id == id).unwrap_or(false))
	}

	fn is_flow_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.flow.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|f| f.namespace == namespace && f.name == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackFlowChangeOperations for SubscriptionTransaction {
	fn track_flow_created(&mut self, flow: Flow) -> Result<()> {
		self.inner.track_flow_created(flow)
	}

	fn track_flow_updated(&mut self, pre: Flow, post: Flow) -> Result<()> {
		self.inner.track_flow_updated(pre, post)
	}

	fn track_flow_deleted(&mut self, flow: Flow) -> Result<()> {
		self.inner.track_flow_deleted(flow)
	}
}

impl TransactionalFlowChanges for SubscriptionTransaction {
	fn find_flow(&self, id: FlowId) -> Option<&Flow> {
		self.inner.find_flow(id)
	}

	fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Flow> {
		self.inner.find_flow_by_name(namespace, name)
	}

	fn is_flow_deleted(&self, id: FlowId) -> bool {
		self.inner.is_flow_deleted(id)
	}

	fn is_flow_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_flow_deleted_by_name(namespace, name)
	}
}
