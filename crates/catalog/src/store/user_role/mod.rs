// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::user::UserRoleDef, store::MultiVersionValues};

use crate::store::user_role::schema::user_role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_user_role(multi: MultiVersionValues) -> UserRoleDef {
	let row = multi.values;
	let user_id = user_role::SCHEMA.get_u64(&row, user_role::USER_ID);
	let role_id = user_role::SCHEMA.get_u64(&row, user_role::ROLE_ID);

	UserRoleDef {
		user_id,
		role_id,
	}
}
