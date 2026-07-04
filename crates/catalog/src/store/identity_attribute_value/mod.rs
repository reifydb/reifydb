// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::value::decode_value;
use reifydb_core::{
	interface::{catalog::identity::IdentityAttributeValue, store::MultiVersionRow},
	return_internal_error,
};

use crate::{Result, store::identity_attribute_value::shape::identity_attribute_value};

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity_attribute_value(multi: MultiVersionRow) -> Result<IdentityAttributeValue> {
	let row = multi.row;
	let identity = identity_attribute_value::SHAPE.get_identity_id(&row, identity_attribute_value::IDENTITY);
	let attribute = identity_attribute_value::SHAPE.get_u64(&row, identity_attribute_value::ATTRIBUTE);
	let blob = identity_attribute_value::SHAPE.get_blob(&row, identity_attribute_value::VALUE);
	let value = match decode_value(blob.as_bytes()) {
		Ok(value) => value,
		Err(e) => return_internal_error!("failed to decode identity attribute value: {}", e),
	};

	Ok(IdentityAttributeValue {
		identity,
		attribute,
		value,
	})
}
