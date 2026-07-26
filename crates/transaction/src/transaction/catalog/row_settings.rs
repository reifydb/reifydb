// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackRowSettingsChangeOperations, storage::StorageId},
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
	fn track_row_settings_created(&mut self, storage: StorageId, settings: RowSettings) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((storage, settings)),
			op: Create,
		};
		self.changes.add_row_settings_change(change);
		Ok(())
	}

	fn track_row_settings_updated(
		&mut self,
		storage: StorageId,
		pre: RowSettings,
		post: RowSettings,
	) -> Result<()> {
		let change = Change {
			pre: Some((storage, pre)),
			post: Some((storage, post)),
			op: Update,
		};
		self.changes.add_row_settings_change(change);
		Ok(())
	}

	fn track_row_settings_deleted(&mut self, storage: StorageId, settings: RowSettings) -> Result<()> {
		let change = Change {
			pre: Some((storage, settings)),
			post: None,
			op: Delete,
		};
		self.changes.add_row_settings_change(change);
		Ok(())
	}
}

impl TransactionalRowSettingsChanges for AdminTransaction {
	fn find_row_settings(&self, storage: StorageId) -> Option<&RowSettings> {
		for change in self.changes.row_settings.iter().rev() {
			if let Some((s, settings)) = &change.post {
				if *s == storage {
					return Some(settings);
				}
			} else if let Some((s, _)) = &change.pre
				&& *s == storage && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_row_settings_deleted(&self, storage: StorageId) -> bool {
		self.changes.row_settings.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|(s, _)| *s == storage).unwrap_or(false)
		})
	}
}
