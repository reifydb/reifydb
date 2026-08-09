// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{row::catalog::EncodedCatalogRow, tag::value_type_from_tag_byte};
use reifydb_core::interface::{catalog::identity::IdentityAttribute, store::MultiVersionRow};
use reifydb_value::Result;

use crate::store::identity_attribute::shape::identity_attribute;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity_attribute(multi: MultiVersionRow) -> Result<IdentityAttribute> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = identity_attribute::get_id(&bytes);
	let name = identity_attribute::get_name(&bytes).to_string();
	let value_type = value_type_from_tag_byte(identity_attribute::get_value_type(&bytes));

	Ok(IdentityAttribute {
		id,
		name,
		value_type,
	})
}
