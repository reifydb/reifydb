// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table name mapping for SQLite backend.

use crate::backend::primitive::TableId;

/// Convert TableId to a SQLite table name.
pub(super) fn table_id_to_name(table: TableId) -> String {
	match table {
		TableId::Multi => "prim_multi".to_string(),
		TableId::Single => "prim_single".to_string(),
		TableId::Cdc => "prim_cdc".to_string(),
		TableId::Source(id) => format!("prim_source_{}", id),
		TableId::Operator(id) => format!("prim_operator_{}", id),
	}
}
