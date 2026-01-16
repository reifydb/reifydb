// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{error::diagnostic::Diagnostic, fragment::Fragment};

/// Generic tokenizeer error with custom message
pub fn tokenize_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "AST_001".to_string(),
		statement: None,
		message: format!("tokenizer error: {}", message),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check syntax and token format".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Unexpected end of file during parsing
pub fn unexpected_eof_error() -> Diagnostic {
	Diagnostic {
		code: "AST_002".to_string(),
		statement: None,
		message: "Unexpected end of file".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Complete the statement".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Error for when we expect an identifier token specifically
pub fn expected_identifier_error(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	let label = Some(format!("found `{}`", value));

	Diagnostic {
		code: "AST_003".to_string(),
		statement: None,
		message: "unexpected token: expected `identifier`".to_string(),
		column: None,
		fragment,
		label,
		help: Some("expected token of type `identifier`".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Error for invalid policy tokens
pub fn invalid_policy_error(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	let message = format!("Invalid policy token: {}", value);
	let label = Some(format!("found `{}`", value));

	Diagnostic {
		code: "AST_004".to_string(),
		statement: None,
		message,
		column: None,
		fragment,
		label,
		help: Some("Expected a valid policy identifier".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Error for unexpected tokens
pub fn unexpected_token_error(expected: &str, fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	let message = format!("Unexpected token: expected {}, got {}", expected, value);
	let label = Some(format!("found `{}`", value));
	Diagnostic {
		code: "AST_005".to_string(),
		statement: None,
		message,
		column: None,
		fragment,
		label,
		help: Some(format!("Use {} instead", expected)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Error for unsupported tokens
pub fn unsupported_token_error(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	let message = format!("Unsupported token: {}", value);
	let label = Some(format!("found `{}`", value));

	Diagnostic {
		code: "AST_006".to_string(),
		statement: None,
		message,
		column: None,
		fragment,
		label,
		help: Some("This token is not supported in this context".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Multiple expressions require curly braces
pub fn multiple_expressions_without_braces(fragment: Fragment) -> Diagnostic {
	let keyword = fragment.text().to_string();
	Diagnostic {
		code: "AST_007".to_string(),
		statement: None,
		message: format!("multiple expressions in `{}` require curly braces", &keyword),
		fragment,
		label: Some("missing `{ … }` around expressions".to_string()),
		help: Some(format!("wrap the expressions in curly braces:\n    {} {{ expr1, expr2, … }}", keyword)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Type not found error
pub fn unrecognized_type(fragment: Fragment) -> Diagnostic {
	let type_name = fragment.text().to_string();
	Diagnostic {
		code: "AST_008".to_string(),
		statement: None,
		message: format!("cannot find type `{}`", &type_name),
		fragment,
		label: Some("type not found".to_string()),
		help: None,
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Unsupported AST operator in logical plan compilation
pub fn unsupported_ast_node(fragment: Fragment, node_type: &str) -> Diagnostic {
	Diagnostic {
		code: "AST_009".to_string(),
		statement: None,
		message: format!("unsupported query syntax: {}", node_type),
		fragment,
		label: Some("not supported in this context".to_string()),
		help: Some("This syntax is not yet supported or may be invalid in this context".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Empty pipeline error
pub fn empty_pipeline_error() -> Diagnostic {
	Diagnostic {
		code: "AST_010".to_string(),
		statement: None,
		message: "empty query pipeline".to_string(),
		fragment: Fragment::None,
		label: None,
		help: Some("A query pipeline must contain at least one operation".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
