// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::error::diagnostic::Diagnostic;

/// Transaction conflict occurred due to concurrent modifications
pub fn transaction_conflict() -> Diagnostic {
    Diagnostic {
        code: "TXN_001".to_string(),
        statement: None,
        message: "Transaction conflict detected - another transaction modified the same data"
            .to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Retry the transaction".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Transaction was discarded due to rollback or abort
pub fn transaction_rolled_back() -> Diagnostic {
    Diagnostic {
        code: "TXN_002".to_string(),
        statement: None,
        message: "Transaction rolled back and cannot be committed".to_string(),
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

/// Cannot perform write operation on a read transaction
pub fn write_on_read_transaction() -> Diagnostic {
    Diagnostic {
        code: "TXN_005".to_string(),
        statement: None,
        message: "Cannot perform write operation on a read transaction".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Use a write transaction for this operation".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Cannot commit a read transaction
pub fn cannot_commit_read_transaction() -> Diagnostic {
    Diagnostic {
        code: "TXN_006".to_string(),
        statement: None,
        message: "Cannot commit a read transaction".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Read transactions do not need to be committed".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Cannot rollback a read transaction
pub fn cannot_rollback_read_transaction() -> Diagnostic {
    Diagnostic {
        code: "TXN_007".to_string(),
        statement: None,
        message: "Cannot rollback a read transaction".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Read transactions do not need to be rolled back".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Transaction was already committed
pub fn transaction_already_committed() -> Diagnostic {
    Diagnostic {
        code: "TXN_008".to_string(),
        statement: None,
        message: "Transaction was already committed".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Cannot use a transaction after it has been committed".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Transaction was already rolled back
pub fn transaction_already_rolled_back() -> Diagnostic {
    Diagnostic {
        code: "TXN_009".to_string(),
        statement: None,
        message: "Transaction was already rolled back".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Cannot use a transaction after it has been rolled back".to_string()),
        notes: vec![],
        cause: None,
    }
}
