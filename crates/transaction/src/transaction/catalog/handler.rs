// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackHandlerChangeOperations,
	handler::Handler,
	id::{HandlerId, NamespaceId},
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalHandlerChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackHandlerChangeOperations for AdminTransaction {
	fn track_handler_created(&mut self, handler: Handler) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(handler),
			op: Create,
		};
		self.changes.add_handler_change(change);
		Ok(())
	}

	fn track_handler_deleted(&mut self, handler: Handler) -> Result<()> {
		let change = Change {
			pre: Some(handler),
			post: None,
			op: Delete,
		};
		self.changes.add_handler_change(change);
		Ok(())
	}
}

impl TransactionalHandlerChanges for AdminTransaction {
	fn find_handler_by_id(&self, id: HandlerId) -> Option<&Handler> {
		for change in self.changes.handler.iter().rev() {
			if let Some(handler) = &change.post {
				if handler.id == id {
					return Some(handler);
				}
			} else if let Some(handler) = &change.pre
				&& handler.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_handler_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Handler> {
		self.changes
			.handler
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|h| h.namespace == namespace && h.name == name))
	}

	fn is_handler_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.handler.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|h| h.namespace == namespace && h.name == name)
					.unwrap_or(false)
		})
	}
}
