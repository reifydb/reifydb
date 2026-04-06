// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackRowTtlChangeOperations, shape::ShapeId},
	row::RowTtl,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalRowTtlChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackRowTtlChangeOperations for AdminTransaction {
	fn track_row_ttl_created(&mut self, shape: ShapeId, ttl: RowTtl) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((shape, ttl)),
			op: Create,
		};
		self.changes.add_row_ttl_change(change);
		Ok(())
	}

	fn track_row_ttl_updated(&mut self, shape: ShapeId, pre: RowTtl, post: RowTtl) -> Result<()> {
		let change = Change {
			pre: Some((shape, pre)),
			post: Some((shape, post)),
			op: Update,
		};
		self.changes.add_row_ttl_change(change);
		Ok(())
	}

	fn track_row_ttl_deleted(&mut self, shape: ShapeId, ttl: RowTtl) -> Result<()> {
		let change = Change {
			pre: Some((shape, ttl)),
			post: None,
			op: Delete,
		};
		self.changes.add_row_ttl_change(change);
		Ok(())
	}
}

impl TransactionalRowTtlChanges for AdminTransaction {
	fn find_row_ttl(&self, shape: ShapeId) -> Option<&RowTtl> {
		for change in self.changes.row_ttl.iter().rev() {
			if let Some((s, ttl)) = &change.post {
				if *s == shape {
					return Some(ttl);
				}
			} else if let Some((s, _)) = &change.pre
				&& *s == shape && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_row_ttl_deleted(&self, shape: ShapeId) -> bool {
		self.changes.row_ttl.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|(s, _)| *s == shape).unwrap_or(false)
		})
	}
}
