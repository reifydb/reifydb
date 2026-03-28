// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::identity::Identity, store::MultiVersionRow};

use crate::store::identity::schema::identity;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_identity(multi: MultiVersionRow) -> Identity {
	let row = multi.row;
	let id = identity::SCHEMA.get_identity_id(&row, identity::IDENTITY);
	let name = identity::SCHEMA.get_utf8(&row, identity::NAME).to_string();
	let enabled = identity::SCHEMA.get_bool(&row, identity::ENABLED);

	Identity {
		id,
		name,
		enabled,
	}
}
