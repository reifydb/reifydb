// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorSettingsChangeOperations, flow::OperatorId},
	row::OperatorSettings,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalOperatorSettingsChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackOperatorSettingsChangeOperations for AdminTransaction {
	fn track_operator_settings_created(&mut self, operator: OperatorId, settings: OperatorSettings) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((operator, settings)),
			op: Create,
		};
		self.changes.add_operator_settings_change(change);
		Ok(())
	}
}

impl TransactionalOperatorSettingsChanges for AdminTransaction {
	fn find_operator_settings(&self, operator: OperatorId) -> Option<&OperatorSettings> {
		for change in self.changes.operator_settings.iter().rev() {
			if let Some((o, settings)) = &change.post {
				if *o == operator {
					return Some(settings);
				}
			} else if let Some((o, _)) = &change.pre
				&& *o == operator && change.op == Delete
			{
				return None;
			}
		}
		None
	}
}
