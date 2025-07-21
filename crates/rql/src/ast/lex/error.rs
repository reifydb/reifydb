// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Re-export core::Error as the unified error type for this module
pub use reifydb_core::Error;

// Helper function to create lexer errors
use reifydb_core::diagnostic::Diagnostic;

pub fn lex_error(message: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "LEX_001".to_string(),
        statement: None,
        message: format!("Lexical error: {}", message),
        column: None,
        span: None,
        label: None,
        help: Some("Check syntax and character encoding".to_string()),
        notes: vec![],
        cause: None,
    })
}