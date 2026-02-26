// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::user::UserDef, store::MultiVersionValues};

use crate::store::user::schema::user;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_user(multi: MultiVersionValues) -> UserDef {
	let row = multi.values;
	let id = user::SCHEMA.get_u64(&row, user::ID);
	let name = user::SCHEMA.get_utf8(&row, user::NAME).to_string();
	let password_hash = user::SCHEMA.get_utf8(&row, user::PASSWORD_HASH).to_string();
	let enabled = user::SCHEMA.get_bool(&row, user::ENABLED);

	UserDef {
		id,
		name,
		password_hash,
		enabled,
	}
}
