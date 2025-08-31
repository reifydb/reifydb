// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::{ColumnDef, SchemaId};

/// Unique identifier for a virtual table type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableVirtualId(pub u64);

/// Definition of a virtual table, similar to TableDef but for virtual tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableVirtualDef {
	/// Virtual table identifier
	pub id: TableVirtualId,
	/// Schema this virtual table belongs to
	pub schema: SchemaId,
	/// Name of the virtual table
	pub name: String,
	/// Column definitions
	pub columns: Vec<ColumnDef>,
	/// Virtual table provider type identifier
	pub provider: String,
}
