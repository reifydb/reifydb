// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::CommitVersion;
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

use crate::dictionary::error::DictionaryError;

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
	#[error("Transaction conflict detected")]
	Conflict,

	#[error("Transaction rolled back and cannot be committed")]
	RolledBack,

	#[error("Transaction contains too many writes and exceeds size limits")]
	TooLarge,

	#[error("Transaction open too long - conflict history has been evicted")]
	TooOld,

	#[error("Transaction was already committed")]
	AlreadyCommitted,

	#[error("Transaction was already rolled back")]
	AlreadyRolledBack,

	#[error("Key '{key}' is not in the transaction's declared key scope")]
	KeyOutOfScope {
		key: String,
	},

	#[error("Transaction was poisoned by a prior error")]
	Poisoned {
		cause: Box<Diagnostic>,
	},

	#[error("Raft proposal failed: {message}")]
	RaftProposeFailed {
		message: String,
	},

	#[error("Snapshot version {} evicted by GC; cutoff is {}", version.0, cutoff.0)]
	SnapshotVersionEvicted {
		version: CommitVersion,
		cutoff: CommitVersion,
	},

	#[error("Database is shutting down; new transactions are rejected")]
	ShuttingDown,

	#[error(transparent)]
	Dictionary(#[from] DictionaryError),
}

impl IntoDiagnostic for TransactionError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			TransactionError::Conflict => Diagnostic {
				code: "TXN_001".to_string(),
				rql: None,
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
				rql: None,
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
				rql: None,
				message: "Transaction contains too many writes and exceeds size limits".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Split the transaction into smaller batches".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::TooOld => Diagnostic {
				code: "TXN_004".to_string(),
				rql: None,
				message: "Transaction open too long - the conflict history for this read snapshot has been evicted".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Start a new transaction".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::AlreadyCommitted => Diagnostic {
				code: "TXN_008".to_string(),
				rql: None,
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
				rql: None,
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
				rql: None,
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

			TransactionError::Poisoned { cause } => Diagnostic {
				code: "TXN_011".to_string(),
				rql: None,
				message: "Transaction was poisoned by a prior error".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("A previous statement failed, invalidating this transaction. Start a new transaction.".to_string()),
				notes: vec![],
				cause: Some(cause),
				operator_chain: None,
			},

			TransactionError::RaftProposeFailed { message } => Diagnostic {
				code: "TXN_013".to_string(),
				rql: None,
				message: format!("Raft proposal failed: {message}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("The write could not be replicated. Retry or check cluster health.".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::SnapshotVersionEvicted { version, cutoff } => Diagnostic {
				code: "TXN_012".to_string(),
				rql: None,
				message: format!(
					"Snapshot version {} evicted by historical GC; current cutoff is {}",
					version.0, cutoff.0
				),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some(
					"Acquire the hydration lease against a more recent version, or subscribe with WITH { hydration: { enabled: false } }."
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::ShuttingDown => Diagnostic {
				code: "TXN_014".to_string(),
				rql: None,
				message: "Database is shutting down; new transactions are rejected".to_string(),
				column: None,
				fragment: Fragment::None,
				label: Some("shutdown in progress".to_string()),
				help: Some("Retry once the instance has finished shutting down or restarted".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TransactionError::Dictionary(err) => err.into_diagnostic(),
		}
	}
}

impl From<TransactionError> for Error {
	fn from(err: TransactionError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl From<DictionaryError> for Error {
	fn from(err: DictionaryError) -> Self {
		TransactionError::from(err).into()
	}
}
