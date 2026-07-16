// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::flow::FlowNodeId, store::EntryKind};

pub(super) fn entry_id_to_name(table: EntryKind) -> String {
	match table {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id) => format!("source_{}", id),
		EntryKind::PartitionedSource(id) => format!("partsource_{}", id),
		EntryKind::Operator(id) => format!("operator_{}", id),
		EntryKind::OperatorInternal(id) => format!("operator_internal_{}", id),
	}
}

pub(super) fn current_table_name(table: EntryKind) -> String {
	format!("{}__current", entry_id_to_name(table))
}

pub(super) fn operator_node_of_table_name(name: &str) -> Option<FlowNodeId> {
	let base = name.strip_suffix("__current")?;
	let id = base.strip_prefix("operator_internal_").or_else(|| base.strip_prefix("operator_"))?;
	id.parse::<u64>().ok().map(FlowNodeId)
}
