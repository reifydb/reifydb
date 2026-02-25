// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{MigrationEventId, MigrationId},
		migration::{MigrationDef, MigrationEvent},
	},
};

use crate::materialized::{MaterializedCatalog, MultiVersionMigrationDef, MultiVersionMigrationEvent};

impl MaterializedCatalog {
	/// Find a migration by ID at a specific version
	pub fn find_migration_at(&self, id: MigrationId, version: CommitVersion) -> Option<MigrationDef> {
		self.migrations.get(&id).and_then(|entry| entry.value().get(version))
	}

	/// Find a migration by name at a specific version
	pub fn find_migration_by_name_at(&self, name: &str, version: CommitVersion) -> Option<MigrationDef> {
		self.migrations_by_name.get(&name.to_string()).and_then(|entry| {
			let migration_id = *entry.value();
			self.find_migration_at(migration_id, version)
		})
	}

	/// Find a migration by name (latest version)
	pub fn find_migration_by_name(&self, name: &str) -> Option<MigrationDef> {
		self.migrations_by_name.get(&name.to_string()).and_then(|entry| {
			let migration_id = *entry.value();
			self.migrations.get(&migration_id).and_then(|m| m.value().get_latest())
		})
	}

	/// List all migrations (latest version of each)
	pub fn list_migrations(&self) -> Vec<MigrationDef> {
		self.migrations.iter().filter_map(|entry| entry.value().get_latest()).collect()
	}

	/// Set a migration definition at a specific version
	pub fn set_migration(&self, id: MigrationId, version: CommitVersion, migration: Option<MigrationDef>) {
		if let Some(entry) = self.migrations.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				self.migrations_by_name.remove(&pre.name);
			}
		}

		let multi = self.migrations.get_or_insert_with(id, MultiVersionMigrationDef::new);
		if let Some(new) = migration {
			self.migrations_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}

	/// Find a migration event by ID at a specific version
	pub fn find_migration_event_at(&self, id: MigrationEventId, version: CommitVersion) -> Option<MigrationEvent> {
		self.migration_events.get(&id).and_then(|entry| entry.value().get(version))
	}

	/// List all migration events (latest version of each)
	pub fn list_migration_events(&self) -> Vec<MigrationEvent> {
		self.migration_events.iter().filter_map(|entry| entry.value().get_latest()).collect()
	}

	/// Set a migration event at a specific version
	pub fn set_migration_event(&self, id: MigrationEventId, version: CommitVersion, event: Option<MigrationEvent>) {
		let multi = self.migration_events.get_or_insert_with(id, MultiVersionMigrationEvent::new);
		if let Some(new) = event {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
