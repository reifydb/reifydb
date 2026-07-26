// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackRowSettingsChangeOperations, object::ObjectId},
	row::RowSettings,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalRowSettingsChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackRowSettingsChangeOperations for AdminTransaction {
	fn track_row_settings_created(&mut self, object: ObjectId, settings: RowSettings) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((object, settings)),
			op: Create,
		};
		self.changes.add_row_settings_change(change);
		Ok(())
	}

	fn track_row_settings_updated(&mut self, object: ObjectId, pre: RowSettings, post: RowSettings) -> Result<()> {
		let change = Change {
			pre: Some((object, pre)),
			post: Some((object, post)),
			op: Update,
		};
		self.changes.add_row_settings_change(change);
		Ok(())
	}

	fn track_row_settings_deleted(&mut self, object: ObjectId, settings: RowSettings) -> Result<()> {
		let change = Change {
			pre: Some((object, settings)),
			post: None,
			op: Delete,
		};
		self.changes.add_row_settings_change(change);
		Ok(())
	}
}

impl TransactionalRowSettingsChanges for AdminTransaction {
	fn find_row_settings(&self, object: ObjectId) -> Option<&RowSettings> {
		for change in self.changes.row_settings.iter().rev() {
			if let Some((s, settings)) = &change.post {
				if *s == object {
					return Some(settings);
				}
			} else if let Some((s, _)) = &change.pre
				&& *s == object && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_row_settings_deleted(&self, object: ObjectId) -> bool {
		self.changes.row_settings.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|(s, _)| *s == object).unwrap_or(false)
		})
	}
}
