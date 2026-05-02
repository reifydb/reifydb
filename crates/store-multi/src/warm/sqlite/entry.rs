// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::store::EntryKind;

pub(super) fn entry_id_to_name(table: EntryKind) -> String {
	match table {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id) => format!("source_{}", id),
		EntryKind::Operator(id) => format!("operator_{}", id),
	}
}

/// Physical SQLite table that holds the latest committed value per logical key
/// in the warm tier. One row per key. No historical chain.
pub(super) fn warm_current_table_name(table: EntryKind) -> String {
	format!("{}__warm_current", entry_id_to_name(table))
}
