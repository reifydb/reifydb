// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::identity::Identity, store::MultiVersionRow};

use crate::store::identity::shape::identity;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity(multi: MultiVersionRow) -> Identity {
	let row = multi.row;
	let id = identity::SHAPE.get_identity_id(&row, identity::IDENTITY);
	let name = identity::SHAPE.get_utf8(&row, identity::NAME).to_string();
	let enabled = identity::SHAPE.get_bool(&row, identity::ENABLED);

	Identity {
		id,
		name,
		enabled,
	}
}
