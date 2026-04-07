// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Table name mapping for SQLite backend.

use reifydb_core::interface::store::EntryKind;

pub(super) fn entry_id_to_name(table: EntryKind) -> String {
	match table {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id) => format!("source_{}", id),
		EntryKind::Operator(id) => format!("operator_{}", id),
	}
}
