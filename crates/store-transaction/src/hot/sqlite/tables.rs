// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table name mapping for SQLite backend.

use crate::tier::Store;

/// Convert TableId to a SQLite table name.
pub(super) fn table_id_to_name(table: Store) -> String {
	match table {
		Store::Multi => "prim_multi".to_string(),
		Store::Single => "prim_single".to_string(),
		Store::Cdc => "prim_cdc".to_string(),
		Store::Source(id) => format!("prim_source_{}", id),
		Store::Operator(id) => format!("prim_operator_{}", id),
	}
}
