// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Diagnostic conversion for lexer errors.

use reifydb_type::{
	Fragment,
	diagnostic::{Diagnostic, IntoDiagnostic},
};

use crate::token::LexError;

impl IntoDiagnostic for LexError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			LexError::UnexpectedChar {
				ch,
				line,
				column,
				..
			} => Diagnostic {
				code: "LEX_001".to_string(),
				statement: None,
				message: format!("unexpected character '{}'", ch),
				column: None,
				fragment: Fragment::statement(ch.to_string(), line, column),
				label: Some("unexpected character".to_string()),
				help: Some("check for typos or invalid characters".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			LexError::UnterminatedString {
				line,
				column,
				..
			} => Diagnostic {
				code: "LEX_002".to_string(),
				statement: None,
				message: "unterminated string literal".to_string(),
				column: None,
				fragment: Fragment::statement("", line, column),
				label: Some("string starts here".to_string()),
				help: Some("add closing quote".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			LexError::UnterminatedQuotedIdentifier {
				line,
				column,
				..
			} => Diagnostic {
				code: "LEX_003".to_string(),
				statement: None,
				message: "unterminated quoted identifier".to_string(),
				column: None,
				fragment: Fragment::statement("", line, column),
				label: Some("quoted identifier starts here".to_string()),
				help: Some("add closing backtick".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			LexError::InvalidNumber {
				text,
				line,
				column,
				..
			} => Diagnostic {
				code: "LEX_004".to_string(),
				statement: None,
				message: format!("invalid number literal '{}'", text),
				column: None,
				fragment: Fragment::statement(text.clone(), line, column),
				label: Some("invalid number".to_string()),
				help: Some("check number format".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			LexError::EmptyVariable {
				line,
				column,
				..
			} => Diagnostic {
				code: "LEX_005".to_string(),
				statement: None,
				message: "empty variable name".to_string(),
				column: None,
				fragment: Fragment::statement("$", line, column),
				label: Some("variable name expected after $".to_string()),
				help: Some("provide a variable name after the $ symbol".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
