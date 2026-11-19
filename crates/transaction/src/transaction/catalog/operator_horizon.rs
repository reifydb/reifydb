// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorHorizonChangeOperations, flow::OperatorId},
	row::OperatorHorizon,
};
use reifydb_value::error::ValueError;

use crate::{
	change::{Change, OperationType::Create},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackOperatorHorizonChangeOperations for AdminTransaction {
	fn track_operator_horizon_created(
		&mut self,
		operator: OperatorId,
		horizon: OperatorHorizon,
	) -> Result<(), ValueError> {
		let change = Change {
			pre: None,
			post: Some((operator, horizon)),
			op: Create,
		};
		self.changes.add_operator_horizon_change(change);
		Ok(())
	}
}
