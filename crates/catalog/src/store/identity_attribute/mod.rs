// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::value_type_from_tag_byte;
use reifydb_core::interface::{catalog::identity::IdentityAttribute, store::MultiVersionRow};

use crate::store::identity_attribute::shape::identity_attribute;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity_attribute(multi: MultiVersionRow) -> IdentityAttribute {
	let bytes = multi.bytes;
	let id = identity_attribute::SHAPE.get::<u64>(&bytes, identity_attribute::ID);
	let name = identity_attribute::SHAPE.get_utf8(&bytes, identity_attribute::NAME).to_string();
	let value_type =
		value_type_from_tag_byte(identity_attribute::SHAPE.get::<u8>(&bytes, identity_attribute::VALUE_TYPE));

	IdentityAttribute {
		id,
		name,
		value_type,
	}
}
