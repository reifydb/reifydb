// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use create::SchemaToCreate;
use reifydb_core::interface::{SchemaDef, SchemaId, Versioned};

use crate::schema::layout::schema;

mod create;
mod find;
mod get;
mod layout;
mod list;

pub(crate) fn convert_schema(versioned: Versioned) -> SchemaDef {
	let row = versioned.row;
	let id = SchemaId(schema::LAYOUT.get_u64(&row, schema::ID));
	let name = schema::LAYOUT.get_utf8(&row, schema::NAME).to_string();

	SchemaDef {
		id,
		name,
	}
}
