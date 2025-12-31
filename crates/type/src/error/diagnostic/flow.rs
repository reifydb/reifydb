// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{
	Fragment,
	error::diagnostic::{Diagnostic, OperatorChainEntry},
	value::DateTime,
};

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

/// Creates a flow operator error diagnostic with source location and operator chain context.
///
/// This error type is used when an error occurs during flow operator execution,
/// providing detailed context including the operator call chain.
pub fn flow_operator_error(
	reason: impl Into<String>,
	file: &str,
	line: u32,
	column: u32,
	function: &str,
	module_path: &str,
	operator_chain: Vec<OperatorChainEntry>,
) -> Diagnostic {
	let reason = reason.into();

	// Generate a unique error ID based on timestamp and location
	let error_id = format!(
		"FLOW-{}-{}:{}",
		DateTime::now().timestamp_millis(),
		file.rsplit('/').next().unwrap_or(file).replace(".rs", ""),
		line
	);

	let detailed_message = format!("Flow operator error [{}]: {}", error_id, reason);

	let location_info =
		format!("Location: {}:{}:{}\nFunction: {}\nModule: {}", file, line, column, function, module_path);

	// Build operator chain description for notes
	let mut notes = vec![format!("Error occurred in function: {}", function)];

	if !operator_chain.is_empty() {
		notes.push("Operator call chain:".to_string());
		for (i, entry) in operator_chain.iter().enumerate() {
			notes.push(format!(
				"  {}. {} (node={}, v{})",
				i + 1,
				entry.operator_name,
				entry.node_id,
				entry.operator_version
			));
		}
	}

	notes.push(format!("Error tracking ID: {}", error_id));

	let help_message = format!(
		"This error occurred during flow operator execution.\n\n\
		 {}\n\n\
		 Version: {}\n\
		 Build: {} ({})\n\
		 Platform: {} {}",
		location_info,
		env!("CARGO_PKG_VERSION"),
		option_env!("GIT_HASH").unwrap_or("unknown"),
		option_env!("BUILD_DATE").unwrap_or("unknown"),
		std::env::consts::OS,
		std::env::consts::ARCH
	);

	Diagnostic {
		code: "FLOW_007".to_string(),
		statement: None,
		message: detailed_message,
		column: None,
		fragment: Fragment::None,
		label: Some(format!("Flow operator error at {}:{}:{}", file, line, column)),
		help: Some(help_message),
		notes,
		cause: None,
		operator_chain: Some(operator_chain),
	}
}

/// Macro to create a flow operator error with automatic source location capture
#[macro_export]
macro_rules! flow_operator {
	($reason:expr, $chain:expr) => {
		$crate::diagnostic::flow::flow_operator_error(
			$reason,
			file!(),
			line!(),
			column!(),
			{
				fn f() {}
				fn type_name_of<T>(_: T) -> &'static str {
					std::any::type_name::<T>()
				}
				let name = type_name_of(f);
				&name[..name.len() - 3]
			},
			module_path!(),
			$chain,
		)
	};
	($chain:expr, $fmt:expr, $($arg:tt)*) => {
		$crate::diagnostic::flow::flow_operator_error(
			format!($fmt, $($arg)*),
			file!(),
			line!(),
			column!(),
			{
				fn f() {}
				fn type_name_of<T>(_: T) -> &'static str {
					std::any::type_name::<T>()
				}
				let name = type_name_of(f);
				&name[..name.len() - 3]
			},
			module_path!(),
			$chain,
		)
	};
}

/// Macro to create a flow operator Error with automatic source location capture
#[macro_export]
macro_rules! flow_operator_error {
	($reason:expr, $chain:expr) => {
		$crate::Error($crate::flow_operator!($reason, $chain))
	};
	($chain:expr, $fmt:expr, $($arg:tt)*) => {
		$crate::Error($crate::flow_operator!($chain, $fmt, $($arg)*))
	};
}

/// Macro to create a flow operator Err result with automatic source location capture
#[macro_export]
macro_rules! flow_operator_err {
	($reason:expr, $chain:expr) => {
		Err($crate::flow_operator_error!($reason, $chain))
	};
	($chain:expr, $fmt:expr, $($arg:tt)*) => {
		Err($crate::flow_operator_error!($chain, $fmt, $($arg)*))
	};
}

/// Macro to return a flow operator error with automatic source location capture
#[macro_export]
macro_rules! return_flow_operator_error {
	($reason:expr, $chain:expr) => {
		return $crate::flow_operator_err!($reason, $chain)
	};
	($chain:expr, $fmt:expr, $($arg:tt)*) => {
		return $crate::flow_operator_err!($chain, $fmt, $($arg)*)
	};
}
