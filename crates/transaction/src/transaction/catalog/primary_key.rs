// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackPrimaryKeyChangeOperations, key::PrimaryKey, shape::ShapeId,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackPrimaryKeyChangeOperations for AdminTransaction {
	fn track_primary_key_created(&mut self, shape: ShapeId, primary_key: PrimaryKey) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((shape, primary_key)),
			op: Create,
		};
		self.changes.add_primary_key_change(change);
		Ok(())
	}

	fn track_primary_key_deleted(&mut self, shape: ShapeId, primary_key: PrimaryKey) -> Result<()> {
		let change = Change {
			pre: Some((shape, primary_key)),
			post: None,
			op: Delete,
		};
		self.changes.add_primary_key_change(change);
		Ok(())
	}
}
