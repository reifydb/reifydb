// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::identity::IdentityAttributeValue, store::MultiVersionRow};

use crate::store::identity_attribute_value::shape::identity_attribute_value;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity_attribute_value(multi: MultiVersionRow) -> IdentityAttributeValue {
	let row = multi.row;
	let identity = identity_attribute_value::SHAPE.get_identity_id(&row, identity_attribute_value::IDENTITY);
	let attribute = identity_attribute_value::SHAPE.get_u64(&row, identity_attribute_value::ATTRIBUTE);
	let value = identity_attribute_value::SHAPE.get_utf8(&row, identity_attribute_value::VALUE).to_string();

	IdentityAttributeValue {
		identity,
		attribute,
		value,
	}
}
