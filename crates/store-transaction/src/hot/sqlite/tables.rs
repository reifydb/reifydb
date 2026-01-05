// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table name mapping for SQLite backend.

use crate::tier::EntryKind;

/// Convert TableId to a SQLite table name.
pub(super) fn table_id_to_name(table: EntryKind) -> String {
	match table {
		EntryKind::Multi => "prim_multi".to_string(),
		EntryKind::Single => "prim_single".to_string(),
		EntryKind::Cdc => "prim_cdc".to_string(),
		EntryKind::Source(id) => format!("prim_source_{}", id),
		EntryKind::Operator(id) => format!("prim_operator_{}", id),
	}
}
