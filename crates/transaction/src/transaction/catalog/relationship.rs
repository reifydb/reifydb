// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackRelationshipChangeOperations,
	id::{NamespaceId, RelationshipId, TableId},
	relationship::Relationship,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalRelationshipChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackRelationshipChangeOperations for AdminTransaction {
	fn track_relationship_created(&mut self, relationship: Relationship) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(relationship),
			op: Create,
		};
		self.changes.add_relationship_change(change);
		Ok(())
	}

	fn track_relationship_deleted(&mut self, relationship: Relationship) -> Result<()> {
		let change = Change {
			pre: Some(relationship),
			post: None,
			op: Delete,
		};
		self.changes.add_relationship_change(change);
		Ok(())
	}
}

impl TransactionalRelationshipChanges for AdminTransaction {
	fn find_relationship(&self, id: RelationshipId) -> Option<&Relationship> {
		for change in self.changes.relationship.iter().rev() {
			if let Some(rel) = &change.post {
				if rel.id == id {
					return Some(rel);
				}
			} else if let Some(rel) = &change.pre
				&& rel.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_relationship_by_name(
		&self,
		namespace: NamespaceId,
		source_table: TableId,
		name: &str,
	) -> Option<&Relationship> {
		self.changes.relationship.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|r| {
				r.namespace == namespace && r.source_table == source_table && r.name == name
			})
		})
	}

	fn is_relationship_deleted(&self, id: RelationshipId) -> bool {
		self.changes
			.relationship
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|r| r.id) == Some(id))
	}

	fn is_relationship_deleted_by_name(&self, namespace: NamespaceId, source_table: TableId, name: &str) -> bool {
		self.changes.relationship.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|r| {
						r.namespace == namespace
							&& r.source_table == source_table && r.name == name
					})
					.unwrap_or(false)
		})
	}
}
