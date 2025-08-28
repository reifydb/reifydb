// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::{ColumnDef, SchemaId, TableId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
	pub id: TableId,
	pub schema: SchemaId,
	pub name: String,
	pub columns: Vec<ColumnDef>,
}
