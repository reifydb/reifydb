// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::{CatalogTrackMigrationChangeOperations, CatalogTrackMigrationEventChangeOperations},
	migration::{Migration, MigrationEvent},
};
use reifydb_value::Result;

use crate::{
	change::{Change, OperationType::Create, TransactionalMigrationChanges},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackMigrationChangeOperations for AdminTransaction {
	fn track_migration_created(&mut self, migration: Migration) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(migration),
			op: Create,
		};
		self.changes.add_migration_change(change);
		Ok(())
	}
}

impl CatalogTrackMigrationEventChangeOperations for AdminTransaction {
	fn track_migration_event_created(&mut self, event: MigrationEvent) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(event),
			op: Create,
		};
		self.changes.add_migration_event_change(change);
		Ok(())
	}
}

impl TransactionalMigrationChanges for AdminTransaction {
	fn find_migration_by_name(&self, name: &str) -> Option<&Migration> {
		self.changes.migration.iter().rev().find_map(|change| change.post.as_ref().filter(|m| m.name == name))
	}
}
