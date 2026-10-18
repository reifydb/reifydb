// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackColumnBlockChangeOperations, column_block::ColumnBlock, id::ColumnBlockId,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalColumnBlockChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackColumnBlockChangeOperations for AdminTransaction {
	fn track_column_block_created(&mut self, block: ColumnBlock) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(block),
			op: Create,
		};
		self.changes.add_column_block_change(change);
		Ok(())
	}

	fn track_column_block_updated(&mut self, pre: ColumnBlock, post: ColumnBlock) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_column_block_change(change);
		Ok(())
	}

	fn track_column_block_deleted(&mut self, block: ColumnBlock) -> Result<()> {
		let change = Change {
			pre: Some(block),
			post: None,
			op: Delete,
		};
		self.changes.add_column_block_change(change);
		Ok(())
	}
}

impl TransactionalColumnBlockChanges for AdminTransaction {
	fn find_column_block(&self, id: ColumnBlockId) -> Option<&ColumnBlock> {
		for change in self.changes.column_block.iter().rev() {
			if let Some(block) = &change.post {
				if block.id == id {
					return Some(block);
				}
			} else if let Some(block) = &change.pre
				&& block.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_column_block_deleted(&self, id: ColumnBlockId) -> bool {
		self.changes
			.column_block
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id) == Some(id))
	}
}
