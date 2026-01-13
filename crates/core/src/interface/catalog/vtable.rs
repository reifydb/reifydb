// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::{ColumnDef, NamespaceId};

/// Unique identifier for a virtual table type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VTableId(pub u64);

impl From<u64> for VTableId {
	fn from(id: u64) -> Self {
		VTableId(id)
	}
}

impl From<VTableId> for u64 {
	fn from(id: VTableId) -> u64 {
		id.0
	}
}

impl VTableId {
	/// Get the inner u64 value.
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

/// Definition of a virtual table, similar to TableDef but for virtual tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VTableDef {
	/// Virtual table identifier
	pub id: VTableId,
	/// Namespace this virtual table belongs to
	pub namespace: NamespaceId,
	/// Name of the virtual table
	pub name: String,
	/// Column definitions
	pub columns: Vec<ColumnDef>,
}
