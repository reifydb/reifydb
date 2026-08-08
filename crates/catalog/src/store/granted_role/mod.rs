// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::identity::GrantedRole, store::MultiVersionRow};

use crate::store::granted_role::shape::granted_role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_granted_role(multi: MultiVersionRow) -> GrantedRole {
	let bytes = multi.bytes;
	let identity = granted_role::get_identity(&bytes);
	let role_id = granted_role::get_role_id(&bytes);

	GrantedRole {
		identity,
		role_id,
	}
}
