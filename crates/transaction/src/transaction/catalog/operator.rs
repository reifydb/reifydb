// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{change::CatalogTrackOperatorChangeOperations, flow::Operator};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackOperatorChangeOperations for AdminTransaction {
	fn track_operator_created(&mut self, node: Operator) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(node),
			op: Create,
		};
		self.changes.add_operator_change(change);
		Ok(())
	}

	fn track_operator_deleted(&mut self, node: Operator) -> Result<()> {
		let change = Change {
			pre: Some(node),
			post: None,
			op: Delete,
		};
		self.changes.add_operator_change(change);
		Ok(())
	}
}
