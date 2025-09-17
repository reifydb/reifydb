// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::{ColumnDef, NamespaceId};

/// Unique identifier for a virtual table type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableVirtualId(pub u64);

impl From<u64> for TableVirtualId {
	fn from(id: u64) -> Self {
		TableVirtualId(id)
	}
}

impl From<TableVirtualId> for u64 {
	fn from(id: TableVirtualId) -> u64 {
		id.0
	}
}

/// Definition of a virtual table, similar to TableDef but for virtual tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableVirtualDef {
	/// Virtual table identifier
	pub id: TableVirtualId,
	/// Namespace this virtual table belongs to
	pub namespace: NamespaceId,
	/// Name of the virtual table
	pub name: String,
	/// Column definitions
	pub columns: Vec<ColumnDef>,
}
