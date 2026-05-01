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

/// Physical SQLite table that holds the latest visible version per logical key.
pub(super) fn current_table_name(table: EntryKind) -> String {
	format!("{}__current", entry_id_to_name(table))
}

/// Physical SQLite table that holds older versions superseded by current,
/// plus any out-of-order writes whose version trails the current.
pub(super) fn historical_table_name(table: EntryKind) -> String {
	format!("{}__historical", entry_id_to_name(table))
}
