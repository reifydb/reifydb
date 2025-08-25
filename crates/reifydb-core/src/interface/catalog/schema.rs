// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::SchemaId;

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaDef {
	pub id: SchemaId,
	pub name: String,
}
