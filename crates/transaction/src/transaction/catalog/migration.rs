// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::{CatalogTrackMigrationChangeOperations, CatalogTrackMigrationEventChangeOperations},
	id::MigrationId,
	migration::{Migration, MigrationEvent},
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalMigrationChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
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

	fn track_migration_deleted(&mut self, migration: Migration) -> Result<()> {
		let change = Change {
			pre: Some(migration),
			post: None,
			op: Delete,
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
	fn find_migration(&self, id: MigrationId) -> Option<&Migration> {
		for change in self.changes.migration.iter().rev() {
			if let Some(migration) = &change.post {
				if migration.id == id {
					return Some(migration);
				}
			} else if let Some(migration) = &change.pre {
				if migration.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_migration_by_name(&self, name: &str) -> Option<&Migration> {
		self.changes.migration.iter().rev().find_map(|change| change.post.as_ref().filter(|m| m.name == name))
	}

	fn is_migration_deleted(&self, id: MigrationId) -> bool {
		self.changes
			.migration
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|m| m.id == id).unwrap_or(false))
	}

	fn is_migration_deleted_by_name(&self, name: &str) -> bool {
		self.changes.migration.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|m| m.name == name).unwrap_or(false)
		})
	}
}

impl CatalogTrackMigrationChangeOperations for SubscriptionTransaction {
	fn track_migration_created(&mut self, migration: Migration) -> Result<()> {
		self.inner.track_migration_created(migration)
	}

	fn track_migration_deleted(&mut self, migration: Migration) -> Result<()> {
		self.inner.track_migration_deleted(migration)
	}
}

impl CatalogTrackMigrationEventChangeOperations for SubscriptionTransaction {
	fn track_migration_event_created(&mut self, event: MigrationEvent) -> Result<()> {
		self.inner.track_migration_event_created(event)
	}
}

impl TransactionalMigrationChanges for SubscriptionTransaction {
	fn find_migration(&self, id: MigrationId) -> Option<&Migration> {
		self.inner.find_migration(id)
	}

	fn find_migration_by_name(&self, name: &str) -> Option<&Migration> {
		self.inner.find_migration_by_name(name)
	}

	fn is_migration_deleted(&self, id: MigrationId) -> bool {
		self.inner.is_migration_deleted(id)
	}

	fn is_migration_deleted_by_name(&self, name: &str) -> bool {
		self.inner.is_migration_deleted_by_name(name)
	}
}
