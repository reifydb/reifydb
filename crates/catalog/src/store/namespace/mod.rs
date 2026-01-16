// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	catalog::{id::NamespaceId, namespace::NamespaceDef},
	store::MultiVersionValues,
};

use crate::store::namespace::layout::namespace;

pub mod create;
pub mod delete;
pub mod find;
pub mod get;
pub mod layout;
pub mod list;

pub(crate) fn convert_namespace(multi: MultiVersionValues) -> NamespaceDef {
	let row = multi.values;
	let id = NamespaceId(namespace::LAYOUT.get_u64(&row, namespace::ID));
	let name = namespace::LAYOUT.get_utf8(&row, namespace::NAME).to_string();

	NamespaceDef {
		id,
		name,
	}
}
