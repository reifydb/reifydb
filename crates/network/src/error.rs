// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Re-export core::Error as the unified error type for this crate
pub use reifydb_core::Error;

// Type alias for backward compatibility
pub type NetworkError = reifydb_core::Error;

// Helper functions to create specific network errors
use reifydb_core::diagnostic::Diagnostic;

pub fn connection_error(message: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "NET_001".to_string(),
        statement: None,
        message: format!("Connection error: {}", message),
        column: None,
        span: None,
        label: None,
        help: Some("Check network connectivity and server status".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn engine_error(message: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "NET_002".to_string(),
        statement: None,
        message: format!("Engine error: {}", message),
        column: None,
        span: None,
        label: None,
        help: None,
        notes: vec![],
        cause: None,
    })
}

pub fn execution_error(diagnostic: reifydb_core::diagnostic::Diagnostic) -> reifydb_core::Error {
    reifydb_core::Error(diagnostic)
}

// Network-specific error conversion functions
pub fn transport_error(err: tonic::transport::Error) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "NET_003".to_string(),
        statement: None,
        message: format!("Transport error: {}", err),
        column: None,
        span: None,
        label: None,
        help: Some("Check network connectivity".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn status_error(err: tonic::Status) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "NET_004".to_string(),
        statement: None,
        message: format!("gRPC status error: {}", err.message()),
        column: None,
        span: None,
        label: None,
        help: Some("Check gRPC service status".to_string()),
        notes: vec![],
        cause: None,
    })
}