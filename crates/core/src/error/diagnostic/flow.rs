// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::Diagnostic, fragment::Fragment, value::value_type::ValueType};

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

pub fn flow_window_timestamp_column_type_mismatch(column: &str, found: ValueType) -> Diagnostic {
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

pub fn flow_sort_must_be_terminal() -> Diagnostic {
	Diagnostic {
		code: "FLOW_012".to_string(),
		rql: None,
		message: "sort is only supported as the final operator in a view".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some(
			"Move the sort to the end of the pipeline so its output is not consumed by another operator. \
			A view may sort its result, but cannot apply further operators after a sort."
				.to_string(),
		),
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

pub fn flow_unsupported_aggregate_expression(output: &str) -> Diagnostic {
	Diagnostic {
		code: "FLOW_013".to_string(),
		rql: None,
		message: format!("aggregate output '{}' is not a supported aggregate expression in a view", output),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Window and aggregate views support math::count, math::sum, math::avg, math::min and \
			math::max over a column or scalar expression, optionally combined with arithmetic \
			(for example math::max(x) - math::min(x)). Every output must reduce to such an aggregate."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_invalid_worker_id(worker_id: usize, num_workers: usize) -> Diagnostic {
	Diagnostic {
		code: "FLOW_014".to_string(),
		rql: None,
		message: format!("invalid flow worker id {} (pool has {} workers)", worker_id, num_workers),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow scheduler routed a batch to a worker index outside the pool. \
			This indicates a scheduling or rebalance bug in the flow coordinator."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_worker_stopped(worker_id: usize) -> Diagnostic {
	Diagnostic {
		code: "FLOW_015".to_string(),
		rql: None,
		message: format!("flow worker {} has stopped", worker_id),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("A flow pool worker actor is no longer running. \
			This typically occurs during shutdown or after a worker panic."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_pool_busy() -> Diagnostic {
	Diagnostic {
		code: "FLOW_016".to_string(),
		rql: None,
		message: "flow pool is busy processing another batch".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow pool received a request while already processing one. \
			Requests to the pool must be serialized by the coordinator."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_coordinator_busy() -> Diagnostic {
	Diagnostic {
		code: "FLOW_017".to_string(),
		rql: None,
		message: "flow coordinator is busy and already has a deferred consume".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The coordinator received a consume while one was already in flight and another deferred. \
			The CDC consumer must not deliver a third batch concurrently."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_pool_actor_stopped() -> Diagnostic {
	Diagnostic {
		code: "FLOW_018".to_string(),
		rql: None,
		message: "flow pool actor has stopped".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow pool actor is no longer reachable. \
			This typically occurs during shutdown or after a pool panic."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_coordinator_stopped() -> Diagnostic {
	Diagnostic {
		code: "FLOW_020".to_string(),
		rql: None,
		message: "flow coordinator actor has stopped".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("The flow coordinator actor is no longer reachable and cannot consume CDC. \
			This typically occurs during shutdown or after a coordinator panic."
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_worker_failed(worker_id: usize, cause: Diagnostic) -> Diagnostic {
	Diagnostic {
		code: "FLOW_019".to_string(),
		rql: None,
		message: format!("flow worker {} failed", worker_id),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some(
			"A flow pool worker returned an error while processing its batch. See the underlying cause."
				.to_string(),
		),
		notes: vec![],
		cause: Some(Box::new(cause)),
		operator_chain: None,
	}
}

fn flow_diagnostic(code: &str, message: String, help: &str) -> Diagnostic {
	Diagnostic {
		code: code.to_string(),
		rql: None,
		message,
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some(help.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_state_encode_failed(state: &str, cause: String) -> Diagnostic {
	flow_diagnostic(
		"FLOW_021",
		format!("failed to serialize flow operator state '{}': {}", state, cause),
		"An operator failed to encode its persistent state. This usually indicates a bug in the operator's \
		 state serialization, not user input.",
	)
}

pub fn flow_state_decode_failed(state: &str, cause: String) -> Diagnostic {
	flow_diagnostic(
		"FLOW_022",
		format!("failed to deserialize flow operator state '{}': {}", state, cause),
		"An operator failed to decode its persistent state. This may indicate on-disk state corruption or a \
		 state-format change between versions.",
	)
}

pub fn flow_unsupported_node(kind: &str) -> Diagnostic {
	flow_diagnostic(
		"FLOW_023",
		format!("flow node kind '{}' is not supported in persistent flows", kind),
		"This node kind cannot appear in a persistent view flow. Rewrite the view without it.",
	)
}

pub fn flow_node_input_arity(node: &str, expected: &str, found: usize) -> Diagnostic {
	flow_diagnostic(
		"FLOW_024",
		format!("flow node '{}' requires {} inputs, but the DAG provided {}", node, expected, found),
		"The compiled flow DAG has the wrong number of input edges for this node. This indicates a flow \
		 compiler or catalog inconsistency.",
	)
}

pub fn flow_parent_operator_not_found(input: String) -> Diagnostic {
	flow_diagnostic(
		"FLOW_025",
		format!("parent operator not found while wiring flow node input: {}", input),
		"A flow node references a parent operator that has not been registered. The flow DAG is incomplete \
		 or nodes were registered out of order.",
	)
}

pub fn flow_unknown_operator(operator: &str) -> Diagnostic {
	flow_diagnostic(
		"FLOW_026",
		format!("unknown flow operator '{}'", operator),
		"The flow references an operator that is not registered in this build. Check for a missing native \
		 operator or a typo in the operator name.",
	)
}

pub fn flow_ffi_unsupported_on_wasm() -> Diagnostic {
	flow_diagnostic(
		"FLOW_027",
		"FFI operators are not supported on the wasm target".to_string(),
		"Native/FFI operators cannot be loaded in a wasm runtime. Use only built-in operators.",
	)
}

pub fn flow_missing_input_edge() -> Diagnostic {
	flow_diagnostic(
		"FLOW_028",
		"flow node is missing a required input edge; the flow DAG is incomplete".to_string(),
		"The compiled flow DAG is missing an edge that a node requires. This indicates a flow compiler bug.",
	)
}

pub fn flow_unknown_diff_origin(operator: &str, origin: Option<String>) -> Diagnostic {
	let message = match origin {
		Some(o) => format!("{} operator received a diff from an unknown node: {}", operator, o),
		None => format!("{} operator received a diff from an unknown node", operator),
	};
	flow_diagnostic(
		"FLOW_029",
		message,
		"An operator received change data tagged with an origin it does not have wired as an input. This \
		 indicates a flow routing or DAG inconsistency.",
	)
}

pub fn flow_sink_view_not_visible_at_registration(flow_id: u64, view_id: u64) -> Diagnostic {
	flow_diagnostic(
		"FLOW_030",
		format!(
			"transactional flow {} references sink view {} that is not visible to its registration query",
			flow_id, view_id
		),
		"A freshly created view must be findable when its flow is registered; otherwise the transactional \
		 view is silently left unmaterialized. This indicates a registration-ordering bug.",
	)
}

pub fn native_abi_tag_mismatch(plugin: u32, host: u32) -> Diagnostic {
	flow_diagnostic(
		"FLOW_031",
		format!("native operator ABI tag mismatch: plugin reports {:#06x}, host expects {:#06x}", plugin, host),
		"The native operator library was built against a different ABI than this host. Rebuild the native \
		 operators against the current version.",
	)
}

pub fn native_library_not_loaded(path: &str) -> Diagnostic {
	flow_diagnostic(
		"FLOW_032",
		format!("native operator library not loaded: {}", path),
		"The native operator shared library could not be loaded. Check that the .so exists and is readable.",
	)
}

pub fn native_symbol_not_found(symbol: &str, cause: String) -> Diagnostic {
	flow_diagnostic(
		"FLOW_033",
		format!("native operator symbol '{}' not found: {}", symbol, cause),
		"The native operator library is missing an expected symbol. It may be built against a different ABI \
		 or be the wrong library.",
	)
}

pub fn native_operator_not_found(operator: &str) -> Diagnostic {
	flow_diagnostic(
		"FLOW_034",
		format!("native operator '{}' not found", operator),
		"No loaded native library provides this operator. Check the operators directory and the operator name.",
	)
}

pub fn native_create_failed(cause: String) -> Diagnostic {
	flow_diagnostic(
		"FLOW_035",
		format!("failed to create native/FFI operator: {}", cause),
		"The native operator's create function returned an error. See the underlying cause.",
	)
}

pub fn flow_sink_missing_system_column(column: &str, row_idx: usize) -> Diagnostic {
	flow_diagnostic(
		"FLOW_036",
		format!("row at index {} is missing the '{}' system column", row_idx, column),
		"A view sink row is missing a required system timestamp column. This indicates an encoding bug \
		 upstream of the sink.",
	)
}

pub fn flow_sink_dictionary_not_found(dictionary_id: String, column: &str) -> Diagnostic {
	flow_diagnostic(
		"FLOW_037",
		format!("dictionary {} not found for view column '{}'", dictionary_id, column),
		"A dictionary-encoded view column references a dictionary that no longer exists in the catalog.",
	)
}
