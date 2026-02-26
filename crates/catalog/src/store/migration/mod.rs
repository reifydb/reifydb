// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::encoded::EncodedValues,
	interface::catalog::{
		id::{MigrationEventId, MigrationId},
		migration::{MigrationAction, MigrationDef, MigrationEvent},
	},
};
use schema::{migration, migration_event};

pub mod create;
pub mod find;
pub mod list;
pub(crate) mod schema;

pub(crate) fn migration_def_from_row(row: &EncodedValues) -> MigrationDef {
	let id = MigrationId(migration::SCHEMA.get_u64(row, migration::ID));
	let name = migration::SCHEMA.get_utf8(row, migration::NAME).to_string();
	let body = migration::SCHEMA.get_utf8(row, migration::BODY).to_string();
	let rollback_body = {
		let s = migration::SCHEMA.get_utf8(row, migration::ROLLBACK_BODY);
		if s.is_empty() {
			None
		} else {
			Some(s.to_string())
		}
	};

	MigrationDef {
		id,
		name,
		body,
		rollback_body,
	}
}

pub(crate) fn migration_event_from_row(row: &EncodedValues) -> MigrationEvent {
	let id = MigrationEventId(migration_event::SCHEMA.get_u64(row, migration_event::ID));
	let migration_id = MigrationId(migration_event::SCHEMA.get_u64(row, migration_event::MIGRATION_ID));
	let action = match migration_event::SCHEMA.get_u8(row, migration_event::ACTION) {
		0 => MigrationAction::Applied,
		_ => MigrationAction::Rollback,
	};

	MigrationEvent {
		id,
		migration_id,
		action,
	}
}
