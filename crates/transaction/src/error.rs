// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
	#[error("Transaction conflict detected")]
	Conflict,

	#[error("Transaction rolled back and cannot be committed")]
	RolledBack,

	#[error("Transaction contains too many writes and exceeds size limits")]
	TooLarge,

	#[error("Transaction was already committed")]
	AlreadyCommitted,

	#[error("Transaction was already rolled back")]
	AlreadyRolledBack,

	#[error("Key '{key}' is not in the transaction's declared key scope")]
	KeyOutOfScope {
		key: String,
	},
}

impl IntoDiagnostic for TransactionError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			TransactionError::Conflict => Diagnostic {
				code: "TXN_001".to_string(),
				statement: None,
				message: "Transaction conflict detected - another transaction modified the same data".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Retry the transaction".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::RolledBack => Diagnostic {
				code: "TXN_002".to_string(),
				statement: None,
				message: "Transaction rolled back and cannot be committed".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Start a new transaction".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::TooLarge => Diagnostic {
				code: "TXN_003".to_string(),
				statement: None,
				message: "Transaction contains too many writes and exceeds size limits".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Split the transaction into smaller batches".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::AlreadyCommitted => Diagnostic {
				code: "TXN_008".to_string(),
				statement: None,
				message: "Transaction was already committed".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Cannot use a transaction after it has been committed".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::AlreadyRolledBack => Diagnostic {
				code: "TXN_009".to_string(),
				statement: None,
				message: "Transaction was already rolled back".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Cannot use a transaction after it has been rolled back".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::KeyOutOfScope { key } => Diagnostic {
				code: "TXN_010".to_string(),
				statement: None,
				message: format!("Key '{}' is not in the transaction's declared key scope", key),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some(
					"Declare the key when beginning the transaction or use a different transaction scope"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<TransactionError> for Error {
	fn from(err: TransactionError) -> Self {
		Error(err.into_diagnostic())
	}
}
