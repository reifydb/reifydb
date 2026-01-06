// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table name mapping for SQLite backend.

use crate::tier::EntryKind;

pub(super) fn entry_id_to_name(table: EntryKind) -> String {
	match table {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Single => "single".to_string(),
		EntryKind::Cdc => "cdc".to_string(),
		EntryKind::Source(id) => format!("source_{}", id),
		EntryKind::Operator(id) => format!("operator_{}", id),
	}
}
