// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::encoded::EncodedValues,
	interface::catalog::{
		handler::HandlerDef,
		id::{HandlerId, NamespaceId},
	},
};
use reifydb_type::value::sumtype::SumTypeId;
use schema::handler;

pub mod create;
pub mod find;
pub mod get;
pub(crate) mod schema;

pub(crate) fn handler_def_from_row(row: &EncodedValues) -> HandlerDef {
	let id = HandlerId(handler::SCHEMA.get_u64(row, handler::ID));
	let namespace = NamespaceId(handler::SCHEMA.get_u64(row, handler::NAMESPACE));
	let name = handler::SCHEMA.get_utf8(row, handler::NAME).to_string();
	let on_sumtype_id = SumTypeId(handler::SCHEMA.get_u64(row, handler::ON_SUMTYPE_ID));
	let on_variant_tag = handler::SCHEMA.get_u8(row, handler::ON_VARIANT_TAG);
	let body_source = handler::SCHEMA.get_utf8(row, handler::BODY_SOURCE).to_string();

	HandlerDef {
		id,
		namespace,
		name,
		on_sumtype_id,
		on_variant_tag,
		body_source,
	}
}
