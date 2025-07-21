// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

// Re-export core::Error as the unified error type for this module
pub use reifydb_core::Error;

// Helper functions to create specific transaction errors
use reifydb_core::diagnostic::Diagnostic;

pub fn transaction_conflict() -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "TXN_001".to_string(),
        statement: None,
        message: "Transaction conflict detected - another transaction modified the same data".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Retry the transaction".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn transaction_discarded() -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "TXN_002".to_string(),
        statement: None,
        message: "Transaction has been discarded and cannot be reused".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Create a new transaction".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn transaction_too_large() -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "TXN_003".to_string(),
        statement: None,
        message: "Transaction contains too many writes and exceeds size limits".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Split the transaction into smaller batches".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn commit_failed(reason: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "TXN_004".to_string(),
        statement: None,
        message: format!("Transaction commit failed: {}", reason),
        column: None,
        span: None,
        label: None,
        help: Some("Check transaction state and retry if appropriate".to_string()),
        notes: vec![],
        cause: None,
    })
}