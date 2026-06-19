// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorSettingsChangeOperations, flow::FlowNodeId},
	row::OperatorSettings,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalOperatorSettingsChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackOperatorSettingsChangeOperations for AdminTransaction {
	fn track_operator_settings_created(&mut self, operator: FlowNodeId, settings: OperatorSettings) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((operator, settings)),
			op: Create,
		};
		self.changes.add_operator_settings_change(change);
		Ok(())
	}

	fn track_operator_settings_updated(
		&mut self,
		operator: FlowNodeId,
		pre: OperatorSettings,
		post: OperatorSettings,
	) -> Result<()> {
		let change = Change {
			pre: Some((operator, pre)),
			post: Some((operator, post)),
			op: Update,
		};
		self.changes.add_operator_settings_change(change);
		Ok(())
	}

	fn track_operator_settings_deleted(&mut self, operator: FlowNodeId, settings: OperatorSettings) -> Result<()> {
		let change = Change {
			pre: Some((operator, settings)),
			post: None,
			op: Delete,
		};
		self.changes.add_operator_settings_change(change);
		Ok(())
	}
}

impl TransactionalOperatorSettingsChanges for AdminTransaction {
	fn find_operator_settings(&self, operator: FlowNodeId) -> Option<&OperatorSettings> {
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

	fn is_operator_settings_deleted(&self, operator: FlowNodeId) -> bool {
		self.changes.operator_settings.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|(o, _)| *o == operator).unwrap_or(false)
		})
	}
}
