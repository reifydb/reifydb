// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Re-export core::Error as the unified error type for this module
pub use reifydb_core::Error;

// Helper functions to create specific parse errors
use crate::ast::lex::{Token, TokenKind};
use reifydb_core::diagnostic::Diagnostic;

pub fn invalid_type_error(got: Token) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "PA_001".to_string(),
        statement: None,
        message: format!("Invalid type token: {:?}", got),
        column: None,
        span: None,
        label: None,
        help: Some("Expected a valid type identifier".to_string()),
        notes: vec![],
        cause: None,
    })
}

// Error for when we expect an identifier token specifically  
pub fn expected_identifier_error(got: Token) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "PA_001".to_string(),
        statement: None,
        message: "unexpected token: expected `identifier`".to_string(),
        column: None,
        span: Some(got.span.clone()),
        label: Some(format!("found `{}`", got.span.fragment)),
        help: Some("expected token of type `identifier`".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn unexpected_eof_error() -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "PA_002".to_string(),
        statement: None,
        message: "Unexpected end of file".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Complete the statement".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn invalid_policy_error(got: Token) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "PA_003".to_string(),
        statement: None,
        message: format!("Invalid policy token: {:?}", got),
        column: None,
        span: None,
        label: None,
        help: Some("Expected a valid policy identifier".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn unexpected_token_error(expected: TokenKind, got: Token) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "PA_004".to_string(),
        statement: None,
        message: format!("Unexpected token: expected {:?}, got {:?}", expected, got),
        column: None,
        span: None,
        label: None,
        help: Some(format!("Use {:?} instead", expected)),
        notes: vec![],
        cause: None,
    })
}

pub fn unsupported_token_error(got: Token) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "PA_005".to_string(),
        statement: None,
        message: format!("Unsupported token: {:?}", got),
        column: None,
        span: None,
        label: None,
        help: Some("This token is not supported in this context".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn passthrough_error(diagnostic: reifydb_core::diagnostic::Diagnostic) -> reifydb_core::Error {
    reifydb_core::Error(diagnostic)
}