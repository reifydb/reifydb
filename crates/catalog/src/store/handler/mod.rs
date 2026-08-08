// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::interface::catalog::{
	handler::Handler,
	id::{HandlerId, NamespaceId},
};
use reifydb_value::value::sumtype::{SumTypeId, VariantRef};
use shape::handler;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub(crate) mod shape;

pub(crate) fn handler_from_row(bytes: &EncodedBytes) -> Handler {
	let id = HandlerId(handler::get_id(bytes));
	let namespace = NamespaceId(handler::get_namespace(bytes));
	let name = handler::get_name(bytes).to_string();
	let variant = VariantRef {
		sumtype_id: SumTypeId(handler::get_on_sumtype_id(bytes)),
		variant_tag: handler::get_on_variant_tag(bytes),
	};
	let body_source = handler::get_body_source(bytes).to_string();

	Handler {
		id,
		namespace,
		name,
		variant,
		body_source,
	}
}
