// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use create::NamespaceToCreate;
use reifydb_core::interface::{MultiVersionRow, NamespaceDef, NamespaceId};

use crate::namespace::layout::namespace;

mod create;
mod find;
mod get;
mod layout;
mod list;

pub(crate) fn convert_namespace(multi: MultiVersionRow) -> NamespaceDef {
	let row = multi.row;
	let id = NamespaceId(namespace::LAYOSVT.get_u64(&row, namespace::ID));
	let name = namespace::LAYOSVT.get_utf8(&row, namespace::NAME).to_string();

	NamespaceDef {
		id,
		name,
	}
}
