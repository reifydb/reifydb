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
	let bytes = multi.bytes;
	let id = identity::SHAPE.get::<IdentityId>(&bytes, identity::IDENTITY);
	let name = identity::SHAPE.get_utf8(&bytes, identity::NAME).to_string();
	let enabled = identity::SHAPE.get::<bool>(&bytes, identity::ENABLED);

	Identity {
		id,
		name,
		enabled,
	}
}
