// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::identity::IdentityAttribute, store::MultiVersionRow};
use reifydb_value::value::value_type::ValueType;

use crate::store::identity_attribute::shape::identity_attribute;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity_attribute(multi: MultiVersionRow) -> IdentityAttribute {
	let row = multi.row;
	let id = identity_attribute::SHAPE.get_u64(&row, identity_attribute::ID);
	let name = identity_attribute::SHAPE.get_utf8(&row, identity_attribute::NAME).to_string();
	let value_type = ValueType::from_u8(identity_attribute::SHAPE.get_u8(&row, identity_attribute::VALUE_TYPE));

	IdentityAttribute {
		id,
		name,
		value_type,
	}
}
