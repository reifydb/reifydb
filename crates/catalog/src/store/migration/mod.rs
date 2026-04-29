// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		id::{MigrationEventId, MigrationId},
		migration::{Migration, MigrationAction, MigrationEvent},
	},
};
use reifydb_runtime::hash::Hash128;
use shape::{migration, migration_event};

pub mod create;
pub mod find;
pub mod list;
pub(crate) mod shape;

pub(crate) fn migration_from_row(row: &EncodedRow) -> Migration {
	let id = MigrationId(migration::SHAPE.get_u64(row, migration::ID));
	let name = migration::SHAPE.get_utf8(row, migration::NAME).to_string();
	let body = migration::SHAPE.get_utf8(row, migration::BODY).to_string();
	let rollback_body = {
		let s = migration::SHAPE.get_utf8(row, migration::ROLLBACK_BODY);
		if s.is_empty() {
			None
		} else {
			Some(s.to_string())
		}
	};
	let hash = Hash128(migration::SHAPE.get_u128(row, migration::HASH));

	Migration {
		id,
		name,
		body,
		rollback_body,
		hash,
	}
}

pub(crate) fn migration_event_from_row(row: &EncodedRow) -> MigrationEvent {
	let id = MigrationEventId(migration_event::SHAPE.get_u64(row, migration_event::ID));
	let migration_id = MigrationId(migration_event::SHAPE.get_u64(row, migration_event::MIGRATION_ID));
	let action = match migration_event::SHAPE.get_u8(row, migration_event::ACTION) {
		0 => MigrationAction::Applied,
		_ => MigrationAction::Rollback,
	};

	MigrationEvent {
		id,
		migration_id,
		action,
	}
}
