// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::identity::IdentityRoleDef, store::MultiVersionRow};

use crate::store::identity_role::schema::identity_role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_identity_role(multi: MultiVersionRow) -> IdentityRoleDef {
	let row = multi.row;
	let identity = identity_role::SCHEMA.get_identity_id(&row, identity_role::IDENTITY);
	let role_id = identity_role::SCHEMA.get_u64(&row, identity_role::ROLE_ID);

	IdentityRoleDef {
		identity,
		role_id,
	}
}
