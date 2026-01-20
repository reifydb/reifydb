// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::diagnostic::Diagnostic;
use reifydb_type::fragment::Fragment;

/// Transaction conflict occurred due to concurrent modifications
pub fn transaction_conflict() -> Diagnostic {
	Diagnostic {
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
	}
}

/// Transaction was discarded due to rollback or abort
pub fn transaction_rolled_back() -> Diagnostic {
	Diagnostic {
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
	}
}

/// Transaction contains too many writes and exceeds size limits
pub fn transaction_too_large() -> Diagnostic {
	Diagnostic {
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
	}
}

/// Transaction commit failed for a specific reason
pub fn commit_failed(reason: String) -> Diagnostic {
	Diagnostic {
		code: "TXN_004".to_string(),
		statement: None,
		message: format!("Transaction commit failed: {}", reason),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check transaction state and retry if appropriate".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Transaction was already committed
pub fn transaction_already_committed() -> Diagnostic {
	Diagnostic {
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
	}
}

/// Transaction was already rolled back
pub fn transaction_already_rolled_back() -> Diagnostic {
	Diagnostic {
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
	}
}

/// Attempted to access a key outside the transaction's declared key scope
pub fn key_out_of_scope(key: String) -> Diagnostic {
	Diagnostic {
		code: "TXN_010".to_string(),
		statement: None,
		message: format!("Key '{}' is not in the transaction's declared key scope", key),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Declare the key when beginning the transaction or use a different transaction scope"
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
