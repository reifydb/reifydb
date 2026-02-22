// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	catalog::{id::NamespaceId, namespace::NamespaceDef},
	store::MultiVersionValues,
};

use crate::store::namespace::schema::namespace;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub mod schema;

pub(crate) fn convert_namespace(multi: MultiVersionValues) -> NamespaceDef {
	let row = multi.values;
	let id = NamespaceId(namespace::SCHEMA.get_u64(&row, namespace::ID));
	let name = namespace::SCHEMA.get_utf8(&row, namespace::NAME).to_string();
	let parent_id = NamespaceId(namespace::SCHEMA.get_u64(&row, namespace::PARENT_ID));

	NamespaceDef {
		id,
		name,
		parent_id,
	}
}
