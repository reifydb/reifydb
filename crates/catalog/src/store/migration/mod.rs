// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::interface::catalog::{
	id::{MigrationEventId, MigrationId},
	migration::{Migration, MigrationAction, MigrationEvent},
};
use reifydb_value::util::hash::Hash128;
use shape::{migration, migration_event};

pub mod create;
pub mod find;
pub mod list;
pub(crate) mod shape;

pub(crate) fn migration_from_row(bytes: &EncodedCatalogRow) -> Migration {
	let id = MigrationId(migration::get_id(bytes));
	let name = migration::get_name(bytes).to_string();
	let body = migration::get_body(bytes).to_string();
	let rollback_body = {
		let s = migration::get_rollback_body(bytes);
		if s.is_empty() {
			None
		} else {
			Some(s.to_string())
		}
	};
	let hash = Hash128(migration::get_hash(bytes));

	Migration {
		id,
		name,
		body,
		rollback_body,
		hash,
	}
}

pub(crate) fn migration_event_from_row(bytes: &EncodedCatalogRow) -> MigrationEvent {
	let id = MigrationEventId(migration_event::get_id(bytes));
	let migration_id = MigrationId(migration_event::get_migration_id(bytes));
	let action = match migration_event::get_action(bytes) {
		0 => MigrationAction::Applied,
		_ => MigrationAction::Rollback,
	};

	MigrationEvent {
		id,
		migration_id,
		action,
	}
}
