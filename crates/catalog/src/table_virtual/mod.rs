// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{
	QueryTransaction, TableVirtualDef, TableVirtualId,
};

use crate::system::SystemCatalog;

/// Registry for virtual tables
pub struct VirtualTableRegistry;

impl VirtualTableRegistry {
	/// Find a virtual table by its ID
	/// Returns None if the virtual table doesn't exist
	pub fn find_table_virtual(
		_rx: &mut impl QueryTransaction,
		id: TableVirtualId,
	) -> crate::Result<Option<Arc<TableVirtualDef>>> {
		// Currently we only have the sequences virtual table
		if id == crate::system::ids::table_virtual::SEQUENCES {
			Ok(Some(SystemCatalog::sequences()))
		} else {
			Ok(None)
		}
	}

	/// List all virtual tables
	pub fn list_table_virtuals(
		_rx: &mut impl QueryTransaction,
	) -> crate::Result<Vec<Arc<TableVirtualDef>>> {
		// Return all registered virtual tables
		Ok(vec![SystemCatalog::sequences()])
	}
}
