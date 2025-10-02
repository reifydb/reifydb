// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use create::NamespaceToCreate;
use reifydb_core::interface::{MultiVersionValues, NamespaceDef, NamespaceId};

use crate::namespace::layout::namespace;

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
