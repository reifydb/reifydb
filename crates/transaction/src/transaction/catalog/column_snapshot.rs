// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackColumnSnapshotChangeOperations, column_snapshot::ColumnSnapshot, id::ColumnSnapshotId,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalColumnSnapshotChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackColumnSnapshotChangeOperations for AdminTransaction {
	fn track_column_snapshot_created(&mut self, snapshot: ColumnSnapshot) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(snapshot),
			op: Create,
		};
		self.changes.add_column_snapshot_change(change);
		Ok(())
	}

	fn track_column_snapshot_updated(&mut self, pre: ColumnSnapshot, post: ColumnSnapshot) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_column_snapshot_change(change);
		Ok(())
	}

	fn track_column_snapshot_deleted(&mut self, snapshot: ColumnSnapshot) -> Result<()> {
		let change = Change {
			pre: Some(snapshot),
			post: None,
			op: Delete,
		};
		self.changes.add_column_snapshot_change(change);
		Ok(())
	}
}

impl TransactionalColumnSnapshotChanges for AdminTransaction {
	fn find_column_snapshot(&self, id: ColumnSnapshotId) -> Option<&ColumnSnapshot> {
		for change in self.changes.column_snapshot.iter().rev() {
			if let Some(snapshot) = &change.post {
				if snapshot.id == id {
					return Some(snapshot);
				}
			} else if let Some(snapshot) = &change.pre
				&& snapshot.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_column_snapshot_deleted(&self, id: ColumnSnapshotId) -> bool {
		self.changes
			.column_snapshot
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id) == Some(id))
	}
}
