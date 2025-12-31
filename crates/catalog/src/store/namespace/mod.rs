// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub use create::NamespaceToCreate;
use reifydb_core::interface::{MultiVersionValues, NamespaceDef, NamespaceId};

use crate::store::namespace::layout::namespace;

mod create;
mod find;
mod get;
mod layout;
mod list;

pub(crate) fn convert_namespace(multi: MultiVersionValues) -> NamespaceDef {
	let row = multi.values;
	let id = NamespaceId(namespace::LAYOUT.get_u64(&row, namespace::ID));
	let name = namespace::LAYOUT.get_utf8(&row, namespace::NAME).to_string();

	NamespaceDef {
		id,
		name,
	}
}
