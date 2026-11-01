// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{reader::Reader, row::catalog::EncodedCatalogRow, typeinfo::decode_value_type};
use reifydb_core::interface::{catalog::identity::IdentityAttribute, store::MultiVersionRow};
use reifydb_value::Result;

use crate::store::identity_attribute::shape::identity_attribute;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity_attribute<K>(multi: MultiVersionRow<K>) -> Result<IdentityAttribute> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = identity_attribute::get_id(&bytes);
	let name = identity_attribute::get_name(&bytes).to_string();
	let value_type = decode_value_type(&mut Reader::new(identity_attribute::get_value_type(&bytes).as_bytes()))
		.unwrap_or_else(|error| panic!("invalid persisted value type of identity attribute {id:?}: {error}"));

	Ok(IdentityAttribute {
		id,
		name,
		value_type,
	})
}
