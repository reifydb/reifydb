// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::clock::Clock;
use reifydb_type::{
	error::{Diagnostic, IntoDiagnostic, util::value_max},
	fragment::Fragment,
};

use crate::error::CoreError;

impl IntoDiagnostic for CoreError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			CoreError::Type(err) => err.into_diagnostic(),

			CoreError::IndexVariableLengthNotSupported => Diagnostic {
				code: "CA_009".to_string(),
				statement: None,
				message: "variable-length types (UTF8, BLOB) are not supported in indexes".to_string(),
				fragment: Fragment::None,
				label: Some("unsupported type for indexing".to_string()),
				help: Some("only fixed-size types can be indexed currently".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::IndexTypesDirectionsMismatch {
				types_len,
				directions_len,
			} => Diagnostic {
				code: "CA_010".to_string(),
				statement: None,
				message: format!(
					"mismatch between number of types ({}) and directions ({})",
					types_len, directions_len
				),
				fragment: Fragment::None,
				label: Some("length mismatch".to_string()),
				help: Some("each indexed field must have a corresponding sort direction".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::FrameError {
				message,
			} => Diagnostic {
				code: "ENG_001".to_string(),
				statement: None,
				message: format!("Frame processing error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check frame data and operations".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::FlowError {
				message,
			} => Diagnostic {
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
			},

			CoreError::FlowTransactionKeyspaceOverlap {
				key,
			} => Diagnostic {
				code: "FLOW_002".to_string(),
				statement: None,
				message: format!(
					"FlowTransaction keyspace overlap: key {} was already written by another FlowTransaction",
					key
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
			},

			CoreError::FlowAlreadyRegistered {
				flow_id,
			} => Diagnostic {
				code: "FLOW_003".to_string(),
				statement: None,
				message: format!("Flow {} is already registered", flow_id),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some(
					"Each flow can only be registered once. Check if the flow is already active."
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::FlowVersionCorrupted {
				flow_id,
				byte_count,
			} => Diagnostic {
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
			},

			CoreError::FlowBackfillTimeout {
				flow_id,
				timeout_secs,
			} => Diagnostic {
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
			},

			CoreError::FlowDispatcherUnavailable => Diagnostic {
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
			},

			CoreError::PrimaryKeyViolation {
				fragment,
				table_name,
				key_columns,
			} => {
				let columns_str = if key_columns.is_empty() {
					"(unknown columns)".to_string()
				} else {
					format!("({})", key_columns.join(", "))
				};
				Diagnostic {
					code: "INDEX_001".to_string(),
					statement: None,
					message: format!(
						"Primary key violation: duplicate key in table '{}' for columns {}",
						table_name, columns_str
					),
					column: None,
					fragment,
					label: Some("primary key violation".to_string()),
					help: Some(format!(
						"A row with the same primary key {} already exists in table '{}'. Primary keys must be unique. Consider using a different value or updating the existing row instead.",
						columns_str, table_name
					)),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CoreError::UniqueIndexViolation {
				fragment,
				table_name,
				index_name,
				key_columns,
			} => {
				let columns_str = if key_columns.is_empty() {
					"(unknown columns)".to_string()
				} else {
					format!("({})", key_columns.join(", "))
				};
				Diagnostic {
					code: "INDEX_002".to_string(),
					statement: None,
					message: format!(
						"Unique index violation: duplicate key in index '{}' on table '{}' for columns {}",
						index_name, table_name, columns_str
					),
					column: None,
					fragment,
					label: Some("unique index violation".to_string()),
					help: Some(format!(
						"A row with the same value for columns {} already exists in table '{}'. The index '{}' requires unique values. Consider using a different value or removing the uniqueness constraint.",
						columns_str, table_name, index_name
					)),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CoreError::Internal {
				message,
				file,
				line,
				column,
				function,
				module_path,
			} => {
				let error_id = format!(
					"ERR-{}-{}:{}",
					Clock::default().now_millis(),
					file.rsplit('/').next().unwrap_or(&file).replace(".rs", ""),
					line
				);
				let detailed_message = format!("Internal error [{}]: {}", error_id, message);
				let location_info = format!(
					"Location: {}:{}:{}\nFunction: {}\nModule: {}",
					file, line, column, function, module_path
				);
				let help_message = format!(
					"This is an internal error that should never occur in normal operation.\n\n\
					 Please file a bug report at: https://github.com/reifydb/reifydb/issues\n\n\
					 Include the following information:\n\
					 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
					 Error ID: {}\n\
					 {}\n\
					 Version: {}\n\
					 Build: {} ({})\n\
					 Platform: {} {}\n\
					 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
					error_id,
					location_info,
					env!("CARGO_PKG_VERSION"),
					option_env!("GIT_HASH").unwrap_or("unknown"),
					option_env!("BUILD_DATE").unwrap_or("unknown"),
					std::env::consts::OS,
					std::env::consts::ARCH
				);
				Diagnostic {
					code: "INTERNAL_ERROR".to_string(),
					statement: None,
					message: detailed_message,
					column: None,
					fragment: Fragment::None,
					label: Some(format!(
						"Internal invariant violated at {}:{}:{}",
						file, line, column
					)),
					help: Some(help_message),
					notes: vec![
						format!("Error occurred in function: {}", function),
						"This error indicates a critical internal inconsistency.".to_string(),
						"Your database may be in an inconsistent state.".to_string(),
						"Consider creating a backup before continuing operations.".to_string(),
						format!("Error tracking ID: {}", error_id),
					],
					cause: None,
					operator_chain: None,
				}
			}

			CoreError::Shutdown {
				component,
			} => Diagnostic {
				code: "SHUTDOWN".to_string(),
				statement: None,
				message: format!("{} is shutting down", component),
				column: None,
				fragment: Fragment::None,
				label: Some(format!("{} is no longer accepting requests", component)),
				help: Some(format!(
					"This operation failed because {} is shutting down.\n\
					 This is expected during database shutdown.",
					component
				)),
				notes: vec![
					"This is not an error - the system is shutting down gracefully".to_string(),
					"Operations submitted during shutdown will be rejected".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			CoreError::SequenceExhausted {
				value_type,
			} => {
				let max = value_max(value_type.clone());
				Diagnostic {
					code: "SEQUENCE_001".to_string(),
					statement: None,
					message: format!("sequence generator of type `{}` is exhausted", value_type),
					fragment: Fragment::None,
					label: Some("no more values can be generated".to_string()),
					help: Some(format!("maximum value for `{}` is `{}`", value_type, max)),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CoreError::CanNotAlterNotAutoIncrement {
				fragment,
			} => {
				let col_name = fragment.text().to_string();
				Diagnostic {
					code: "SEQUENCE_002".to_string(),
					statement: None,
					message: format!(
						"cannot alter sequence for column `{}` which does not have AUTO INCREMENT",
						col_name
					),
					fragment,
					label: Some("column does not have AUTO INCREMENT".to_string()),
					help: Some("only columns with AUTO INCREMENT can have their sequences altered"
						.to_string()),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CoreError::SubsystemInitFailed {
				subsystem,
				reason,
			} => Diagnostic {
				code: "SUB_001".to_string(),
				statement: None,
				message: format!("{} subsystem initialization failed: {}", subsystem, reason),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check subsystem configuration and dependencies".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::SubsystemFeatureDisabled {
				feature,
			} => Diagnostic {
				code: "SUB_002".to_string(),
				statement: None,
				message: format!("Required feature '{}' is not enabled", feature),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Enable the required feature in Cargo.toml".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::SubsystemBindFailed {
				addr,
				reason,
			} => Diagnostic {
				code: "SUB_003".to_string(),
				statement: None,
				message: format!("Failed to bind to {}: {}", addr, reason),
				column: None,
				fragment: Fragment::None,
				label: Some("Check if address is already in use or permissions are insufficient"
					.to_string()),
				help: Some("Try a different port or check firewall settings".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::SubsystemShutdownFailed {
				subsystem,
				reason,
			} => Diagnostic {
				code: "SUB_004".to_string(),
				statement: None,
				message: format!("{} subsystem shutdown failed: {}", subsystem, reason),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::SubsystemAddressUnavailable {
				reason,
			} => Diagnostic {
				code: "SUB_005".to_string(),
				statement: None,
				message: format!("Failed to get local address: {}", reason),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::SubsystemSocketConfigFailed {
				reason,
			} => Diagnostic {
				code: "SUB_006".to_string(),
				statement: None,
				message: format!("Socket configuration failed: {}", reason),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check socket options and system limits".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
