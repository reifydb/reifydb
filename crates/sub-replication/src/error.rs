// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Replication error types and diagnostics.

use std::{error::Error as StdError, fmt, fmt::Display};

use reifydb_core::common::CommitVersion;
use reifydb_type::{error, error::Error};

/// Error type for replication operations.
#[derive(Debug, Clone)]
pub enum ReplicationError {
	/// A CDC entry was applied out of order or duplicated.
	OutOfOrderVersion {
		version: CommitVersion,
		last_applied: CommitVersion,
	},
}

impl Display for ReplicationError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ReplicationError::OutOfOrderVersion {
				version,
				last_applied,
			} => write!(
				f,
				"out-of-order replication version: got {:?}, last applied was {:?}",
				version, last_applied
			),
		}
	}
}

impl StdError for ReplicationError {}

impl From<ReplicationError> for Error {
	fn from(err: ReplicationError) -> Self {
		error!(match err {
			ReplicationError::OutOfOrderVersion {
				version,
				last_applied,
			} => diagnostic::out_of_order_version(version, last_applied),
		})
	}
}

/// Replication-specific diagnostics.
pub mod diagnostic {
	use reifydb_core::common::CommitVersion;
	use reifydb_type::{error::Diagnostic, fragment::Fragment};

	/// A CDC entry was applied out of order or duplicated.
	pub fn out_of_order_version(version: CommitVersion, last_applied: CommitVersion) -> Diagnostic {
		Diagnostic {
			code: "REPL_001".to_string(),
			rql: None,
			message: format!(
				"out-of-order replication version: got {:?}, last applied was {:?}",
				version, last_applied
			),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: Some("log-based replication requires entries to be applied in strict sequential order"
				.to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		}
	}
}
