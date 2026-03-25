// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::user::UserDef, store::MultiVersionRow};

use crate::store::user::schema::user;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_user(multi: MultiVersionRow) -> UserDef {
	let row = multi.row;
	let id = user::SCHEMA.get_u64(&row, user::ID);
	let name = user::SCHEMA.get_utf8(&row, user::NAME).to_string();
	let enabled = user::SCHEMA.get_bool(&row, user::ENABLED);
	let identity = user::SCHEMA.get_identity_id(&row, user::IDENTITY);

	UserDef {
		id,
		identity,
		name,
		enabled,
	}
}
