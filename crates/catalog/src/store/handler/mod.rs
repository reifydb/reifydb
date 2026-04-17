// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		handler::Handler,
		id::{HandlerId, NamespaceId},
	},
};
use reifydb_type::value::sumtype::{SumTypeId, VariantRef};
use shape::handler;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub(crate) mod shape;

pub(crate) fn handler_from_row(row: &EncodedRow) -> Handler {
	let id = HandlerId(handler::SHAPE.get_u64(row, handler::ID));
	let namespace = NamespaceId(handler::SHAPE.get_u64(row, handler::NAMESPACE));
	let name = handler::SHAPE.get_utf8(row, handler::NAME).to_string();
	let variant = VariantRef {
		sumtype_id: SumTypeId(handler::SHAPE.get_u64(row, handler::ON_SUMTYPE_ID)),
		variant_tag: handler::SHAPE.get_u8(row, handler::ON_VARIANT_TAG),
	};
	let body_source = handler::SHAPE.get_utf8(row, handler::BODY_SOURCE).to_string();

	Handler {
		id,
		namespace,
		name,
		variant,
		body_source,
	}
}
