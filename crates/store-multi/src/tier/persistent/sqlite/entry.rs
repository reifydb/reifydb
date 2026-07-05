// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::store::EntryKind;

pub(super) fn entry_id_to_name(table: EntryKind) -> String {
	match table {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id) => format!("source_{}", id),
		EntryKind::Operator(id) => format!("operator_{}", id),
		EntryKind::OperatorInternal(id) => format!("operator_internal_{}", id),
	}
}

pub(super) fn current_table_name(table: EntryKind) -> String {
	format!("{}__current", entry_id_to_name(table))
}
