// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::Diagnostic, fragment::Fragment, value::r#type::Type};

pub fn flow_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "FLOW_001".to_string(),
		rql: None,
		message: format!("Flow processing error: {}", message),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check view flow configuration".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_transaction_keyspace_overlap(key_debug: String) -> Diagnostic {
	Diagnostic {
		code: "FLOW_002".to_string(),
		rql: None,
		message: format!(
			"FlowTransaction keyspace overlap: key {} was already written by another FlowTransaction",
			key_debug
		),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("FlowTransactions must operate on non-overlapping keyspaces. \
			This is typically enforced at the flow scheduler level."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_already_registered(flow_id: u64) -> Diagnostic {
	Diagnostic {
		code: "FLOW_003".to_string(),
		rql: None,
		message: format!("Flow {} is already registered", flow_id),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Each flow can only be registered once. Check if the flow is already active.".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_version_corrupted(flow_id: u64, byte_count: usize) -> Diagnostic {
	Diagnostic {
		code: "FLOW_004".to_string(),
		rql: None,
		message: format!(
			"Flow {} version data is corrupted: expected 8 bytes, found {} bytes",
			flow_id, byte_count
		),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow version stored in the catalog is corrupted. \
			This may indicate data corruption or a shape migration issue. \
			Try dropping and recreating the flow."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_backfill_timeout(flow_id: u64, timeout_secs: u64) -> Diagnostic {
	Diagnostic {
		code: "FLOW_005".to_string(),
		rql: None,
		message: format!(
			"Timeout waiting for flow {} backfill to complete after {} seconds",
			flow_id, timeout_secs
		),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow backfill operation did not complete within the timeout period. \
			This may indicate a large dataset, slow queries, or resource constraints. \
			Try increasing the timeout or check for performance issues."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_dispatcher_unavailable() -> Diagnostic {
	Diagnostic {
		code: "FLOW_006".to_string(),
		rql: None,
		message: "Flow dispatcher is unavailable (channel closed)".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow dispatcher task has stopped or crashed. \
			This may occur during shutdown or if the dispatcher encountered a fatal error. \
			Check dispatcher logs for details."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_remote_source_unsupported() -> Diagnostic {
	Diagnostic {
		code: "FLOW_007".to_string(),
		rql: None,
		message: "Cannot create flow for remote source".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Remote tables do not support local flow graphs. Use remote subscription proxying instead."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_window_timestamp_column_not_found(column: &str) -> Diagnostic {
	Diagnostic {
		code: "FLOW_009".to_string(),
		rql: None,
		message: format!("Window timestamp column '{}' not found in input data", column),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some(format!(
			"The window operator is configured with ts: \"{}\" but no column with that name exists in the source table. \
			Check the column name in the window WITH clause.",
			column
		)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_window_timestamp_column_type_mismatch(column: &str, found: Type) -> Diagnostic {
	Diagnostic {
		code: "FLOW_010".to_string(),
		rql: None,
		message: format!("Window timestamp column '{}' has type {:?}, expected DateTime", column, found),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The timestamp column must be of type DateTime. \
			If you have epoch milliseconds, convert with datetime::from_epoch_millis(column)."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_source_required() -> Diagnostic {
	Diagnostic {
		code: "FLOW_008".to_string(),
		rql: None,
		message: "Flow requires at least one source".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("A view flow must read from a table, view, ring buffer, or series. \
			Inline data (FROM [...]) cannot be used as the source for a view."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_ephemeral_id_capacity_exceeded(flow_id: u64) -> Diagnostic {
	Diagnostic {
		code: "FLOW_011".to_string(),
		rql: None,
		message: format!("Ephemeral flow {} exceeded maximum ID capacity of 99", flow_id),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("An ephemeral flow is limited to 99 nodes and 99 edges. \
			Simplify the subscription query to reduce operator count."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
