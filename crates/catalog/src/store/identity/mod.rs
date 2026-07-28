// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::identity::Identity, store::MultiVersionRow};
use reifydb_value::value::identity::IdentityId;

use crate::store::identity::shape::identity;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity(multi: MultiVersionRow) -> Identity {
	let row = multi.row;
	let id = identity::SHAPE.get::<IdentityId>(&row, identity::IDENTITY);
	let name = identity::SHAPE.get_utf8(&row, identity::NAME).to_string();
	let enabled = identity::SHAPE.get::<bool>(&row, identity::ENABLED);

	Identity {
		id,
		name,
		enabled,
	}
}
