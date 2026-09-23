// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorRetentionChangeOperations, flow::OperatorId},
	row::OperatorRetention,
};
use reifydb_value::Result;

use crate::{
	change::{Change, OperationType::Create},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackOperatorRetentionChangeOperations for AdminTransaction {
	fn track_operator_retention_created(
		&mut self,
		operator: OperatorId,
		retention: OperatorRetention,
	) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((operator, retention)),
			op: Create,
		};
		self.changes.add_operator_retention_change(change);
		Ok(())
	}
}
