// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::interface::{catalog::identity::Identity, store::MultiVersionRow};
use reifydb_value::Result;

use crate::store::identity::shape::identity;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;
pub mod update;

pub(crate) fn convert_identity(multi: MultiVersionRow) -> Result<Identity> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = identity::get_identity(&bytes);
	let name = identity::get_name(&bytes).to_string();
	let enabled = identity::get_enabled(&bytes);
	let kind = identity::get_kind(&bytes);

	Ok(Identity {
		id,
		name,
		enabled,
		kind,
	})
}
