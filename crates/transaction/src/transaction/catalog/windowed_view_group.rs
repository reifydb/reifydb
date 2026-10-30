// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackWindowedViewGroupChangeOperations,
	id::{NamespaceId, WindowedViewGroupId},
	windowed_view_group::WindowedViewGroup,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalWindowedViewGroupChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackWindowedViewGroupChangeOperations for AdminTransaction {
	fn track_windowed_view_group_created(&mut self, group: WindowedViewGroup) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(group),
			op: Create,
		};
		self.changes.add_windowed_view_group_change(change);
		Ok(())
	}

	fn track_windowed_view_group_deleted(&mut self, group: WindowedViewGroup) -> Result<()> {
		let change = Change {
			pre: Some(group),
			post: None,
			op: Delete,
		};
		self.changes.add_windowed_view_group_change(change);
		Ok(())
	}
}

impl TransactionalWindowedViewGroupChanges for AdminTransaction {
	fn find_windowed_view_group(&self, id: WindowedViewGroupId) -> Option<&WindowedViewGroup> {
		for change in self.changes.windowed_view_group.iter().rev() {
			if let Some(group) = &change.post
				&& group.id == id
			{
				return Some(group);
			}
			if let Some(group) = &change.pre
				&& group.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_windowed_view_group_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&WindowedViewGroup> {
		for change in self.changes.windowed_view_group.iter().rev() {
			if let Some(group) = &change.post
				&& group.namespace == namespace
				&& group.name == name
			{
				return Some(group);
			}
			if let Some(group) = &change.pre
				&& group.namespace == namespace
				&& group.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_windowed_view_group_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.windowed_view_group.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|g| g.namespace == namespace && g.name == name)
					.unwrap_or(false)
		})
	}
}
