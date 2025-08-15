// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use create::SchemaToCreate;
use reifydb_core::interface::SchemaId;

mod create;
mod get;
mod layout;

#[derive(Debug, PartialEq)]
pub struct SchemaDef {
	pub id: SchemaId,
	pub name: String,
}
