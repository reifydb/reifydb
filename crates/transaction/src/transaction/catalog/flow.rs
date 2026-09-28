// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackFlowChangeOperations,
	flow::{FlowEntry, FlowId},
	id::NamespaceId,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalFlowChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackFlowChangeOperations for AdminTransaction {
	fn track_flow_created(&mut self, entry: FlowEntry) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(entry),
			op: Create,
		};
		self.changes.add_flow_change(change);
		Ok(())
	}

	fn track_flow_deleted(&mut self, entry: FlowEntry) -> Result<()> {
		let change = Change {
			pre: Some(entry),
			post: None,
			op: Delete,
		};
		self.changes.add_flow_change(change);
		Ok(())
	}
}

impl TransactionalFlowChanges for AdminTransaction {
	fn find_flow(&self, id: FlowId) -> Option<&FlowEntry> {
		for change in self.changes.flow.iter().rev() {
			if let Some(entry) = &change.post
				&& entry.flow.id == id
			{
				return Some(entry);
			}
			if let Some(entry) = &change.pre
				&& entry.flow.id == id
				&& change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&FlowEntry> {
		for change in self.changes.flow.iter().rev() {
			if let Some(entry) = &change.post
				&& entry.flow.namespace == namespace
				&& entry.flow.name == name
			{
				return Some(entry);
			}
			if let Some(entry) = &change.pre
				&& entry.flow.namespace == namespace
				&& entry.flow.name == name
				&& change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_flow_deleted(&self, id: FlowId) -> bool {
		self.changes.flow.iter().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|e| e.flow.id == id).unwrap_or(false)
		})
	}

	fn is_flow_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.flow.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|e| e.flow.namespace == namespace && e.flow.name == name)
					.unwrap_or(false)
		})
	}
}
