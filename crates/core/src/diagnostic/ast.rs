// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::Diagnostic;

/// Generic lexer error with custom message
pub fn lex_error(message: String) -> Diagnostic {
    Diagnostic {
        code: "AST_001".to_string(),
        statement: None,
        message: format!("Lexer error: {}", message),
        column: None,
        span: None,
        label: None,
        help: Some("Check syntax and token format".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Unexpected end of file during parsing
pub fn unexpected_eof_error() -> Diagnostic {
    Diagnostic {
        code: "AST_002".to_string(),
        statement: None,
        message: "Unexpected end of file".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Complete the statement".to_string()),
        notes: vec![],
        cause: None,
    }
}

// Note: Token-specific functions will remain in RQL crate since they depend on RQL types
// These include:
// - invalid_type_error(got: Token) -> requires Token type from RQL
// - expected_identifier_error(got: Token) -> requires Token type from RQL  
// - invalid_policy_error(got: Token) -> requires Token type from RQL
// - unexpected_token_error(expected: TokenKind, got: Token) -> requires TokenKind/Token from RQL
// - unsupported_token_error(got: Token) -> requires Token type from RQL
//
// These will use AST_003 through AST_007 codes but remain as wrapper functions in RQL