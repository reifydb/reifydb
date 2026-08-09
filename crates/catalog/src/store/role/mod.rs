// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::interface::{catalog::identity::Role, store::MultiVersionRow};
use reifydb_value::Result;

use crate::store::role::shape::role;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_role(multi: MultiVersionRow) -> Result<Role> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = role::get_id(&bytes);
	let name = role::get_name(&bytes).to_string();

	Ok(Role {
		id,
		name,
	})
}
