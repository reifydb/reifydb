// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSegmentTreeChangeOperations,
	id::{NamespaceId, SegmentTreeId},
	segment_tree::SegmentTree,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalSegmentTreeChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackSegmentTreeChangeOperations for AdminTransaction {
	fn track_segment_tree_created(&mut self, segment_tree: SegmentTree) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(segment_tree),
			op: Create,
		};
		self.changes.add_segment_tree_change(change);
		Ok(())
	}

	fn track_segment_tree_updated(&mut self, pre: SegmentTree, post: SegmentTree) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_segment_tree_change(change);
		Ok(())
	}

	fn track_segment_tree_deleted(&mut self, segment_tree: SegmentTree) -> Result<()> {
		let change = Change {
			pre: Some(segment_tree),
			post: None,
			op: Delete,
		};
		self.changes.add_segment_tree_change(change);
		Ok(())
	}
}

impl TransactionalSegmentTreeChanges for AdminTransaction {
	fn find_segment_tree(&self, id: SegmentTreeId) -> Option<&SegmentTree> {
		for change in self.changes.segment_trees.iter().rev() {
			if let Some(segment_tree) = &change.post
				&& segment_tree.id == id
			{
				return Some(segment_tree);
			}
			if let Some(segment_tree) = &change.pre
				&& segment_tree.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_segment_tree_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&SegmentTree> {
		for change in self.changes.segment_trees.iter().rev() {
			if let Some(segment_tree) = &change.post
				&& segment_tree.namespace == namespace
				&& segment_tree.name == name
			{
				return Some(segment_tree);
			}
			if let Some(segment_tree) = &change.pre
				&& segment_tree.namespace == namespace
				&& segment_tree.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_segment_tree_deleted(&self, id: SegmentTreeId) -> bool {
		self.changes
			.segment_trees
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id == id).unwrap_or(false))
	}

	fn is_segment_tree_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.segment_trees.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|s| s.namespace == namespace && s.name == name)
					.unwrap_or(false)
		})
	}
}
