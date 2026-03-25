// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::user::UserRoleDef, store::MultiVersionRow};

use crate::store::user_role::schema::user_role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_user_role(multi: MultiVersionRow) -> UserRoleDef {
	let row = multi.row;
	let user_id = user_role::SCHEMA.get_u64(&row, user_role::USER_ID);
	let role_id = user_role::SCHEMA.get_u64(&row, user_role::ROLE_ID);

	UserRoleDef {
		user_id,
		role_id,
	}
}
