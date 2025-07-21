// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::error::diagnostic::Diagnostic;

/// Transaction conflict occurred due to concurrent modifications
pub fn transaction_conflict() -> Diagnostic {
    Diagnostic {
        code: "TXN_001".to_string(),
        statement: None,
        message: "Transaction conflict detected - another transaction modified the same data".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Retry the transaction or use different isolation level".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Transaction was discarded due to rollback or abort
pub fn transaction_discarded() -> Diagnostic {
    Diagnostic {
        code: "TXN_002".to_string(),
        statement: None,
        message: "Transaction was discarded and cannot be committed".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Start a new transaction".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Transaction contains too many writes and exceeds size limits
pub fn transaction_too_large() -> Diagnostic {
    Diagnostic {
        code: "TXN_003".to_string(),
        statement: None,
        message: "Transaction contains too many writes and exceeds size limits".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Split the transaction into smaller batches".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Transaction commit failed for a specific reason
pub fn commit_failed(reason: String) -> Diagnostic {
    Diagnostic {
        code: "TXN_004".to_string(),
        statement: None,
        message: format!("Transaction commit failed: {}", reason),
        column: None,
        span: None,
        label: None,
        help: Some("Check transaction state and retry if appropriate".to_string()),
        notes: vec![],
        cause: None,
    }
}