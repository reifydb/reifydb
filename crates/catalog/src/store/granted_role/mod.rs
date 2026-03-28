// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::identity::GrantedRole, store::MultiVersionRow};

use crate::store::granted_role::schema::granted_role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_granted_role(multi: MultiVersionRow) -> GrantedRole {
	let row = multi.row;
	let identity = granted_role::SCHEMA.get_identity_id(&row, granted_role::IDENTITY);
	let role_id = granted_role::SCHEMA.get_u64(&row, granted_role::ROLE_ID);

	GrantedRole {
		identity,
		role_id,
	}
}
