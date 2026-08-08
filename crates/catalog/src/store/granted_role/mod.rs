// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::identity::GrantedRole, store::MultiVersionRow};
use reifydb_value::value::identity::IdentityId;

use crate::store::granted_role::shape::granted_role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_granted_role(multi: MultiVersionRow) -> GrantedRole {
	let bytes = multi.bytes;
	let identity = granted_role::SHAPE.get::<IdentityId>(&bytes, granted_role::IDENTITY);
	let role_id = granted_role::SHAPE.get::<u64>(&bytes, granted_role::ROLE_ID);

	GrantedRole {
		identity,
		role_id,
	}
}
