// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::user::RoleDef, store::MultiVersionRow};

use crate::store::role::schema::role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_role(multi: MultiVersionRow) -> RoleDef {
	let row = multi.row;
	let id = role::SCHEMA.get_u64(&row, role::ID);
	let name = role::SCHEMA.get_utf8(&row, role::NAME).to_string();

	RoleDef {
		id,
		name,
	}
}
