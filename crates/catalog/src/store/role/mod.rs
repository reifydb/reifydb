// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::identity::Role, store::MultiVersionRow};

use crate::store::role::shape::role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_role(multi: MultiVersionRow) -> Role {
	let row = multi.row;
	let id = role::SHAPE.get_u64(&row, role::ID);
	let name = role::SHAPE.get_utf8(&row, role::NAME).to_string();

	Role {
		id,
		name,
	}
}
