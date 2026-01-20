// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::diagnostic::Diagnostic;
use reifydb_type::fragment::Fragment;

/// View flow processing error
pub fn flow_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "FLOW_001".to_string(),
		statement: None,
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

/// FlowTransaction keyspace overlap detected
pub fn flow_transaction_keyspace_overlap(key_debug: String) -> Diagnostic {
	Diagnostic {
		code: "FLOW_002".to_string(),
		statement: None,
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

/// Flow already registered
pub fn flow_already_registered(flow_id: u64) -> Diagnostic {
	Diagnostic {
		code: "FLOW_003".to_string(),
		statement: None,
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

/// Invalid flow version data in catalog
pub fn flow_version_corrupted(flow_id: u64, byte_count: usize) -> Diagnostic {
	Diagnostic {
		code: "FLOW_004".to_string(),
		statement: None,
		message: format!(
			"Flow {} version data is corrupted: expected 8 bytes, found {} bytes",
			flow_id, byte_count
		),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow version stored in the catalog is corrupted. \
			This may indicate data corruption or a schema migration issue. \
			Try dropping and recreating the flow."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Flow backfill timeout
pub fn flow_backfill_timeout(flow_id: u64, timeout_secs: u64) -> Diagnostic {
	Diagnostic {
		code: "FLOW_005".to_string(),
		statement: None,
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

/// Flow dispatcher unavailable
pub fn flow_dispatcher_unavailable() -> Diagnostic {
	Diagnostic {
		code: "FLOW_006".to_string(),
		statement: None,
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
