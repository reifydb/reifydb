// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;
mod find;
mod get;
pub(crate) mod layout;

pub use create::{ViewColumnToCreate, ViewToCreate};
use layout::view;
use reifydb_core::interface::{SchemaId, Versioned, ViewDef, ViewId, ViewKind};

pub(crate) fn convert_view(versioned: Versioned) -> ViewDef {
	let row = versioned.row;
	let id = ViewId(view::LAYOUT.get_u64(&row, view::ID));
	let schema = SchemaId(view::LAYOUT.get_u64(&row, view::SCHEMA));
	let name = view::LAYOUT.get_utf8(&row, view::NAME).to_string();

	let kind = match view::LAYOUT.get_u8(&row, view::KIND) {
		0 => ViewKind::Deferred,
		1 => ViewKind::Transactional,
		_ => ViewKind::Deferred, /* Default to Deferred for unknown
		                          * values */
	};

	ViewDef {
		id,
		name,
		schema,
		kind,
		columns: vec![], // Columns will be loaded separately if needed
		primary_key: None, /* Primary key will be loaded separately if
		                  * needed */
	}
}
