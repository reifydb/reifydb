// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	OwnedFragment,
	interface::fragment::{IntoFragment, IntoOwnedFragment},
	result::error::diagnostic::Diagnostic,
};

/// Generic lexer error with custom message
pub fn lex_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "AST_001".to_string(),
		statement: None,
		message: format!("Lexer error: {}", message),
		column: None,
		fragment: OwnedFragment::None,
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
		fragment: OwnedFragment::None,
		label: None,
		help: Some("Complete the statement".to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Error for when we expect an identifier token specifically  
pub fn expected_identifier_error(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let value = fragment.value();
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
	}
}

/// Error for invalid policy tokens
pub fn invalid_policy_error(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let value = fragment.value();
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
	}
}

/// Error for unexpected tokens
pub fn unexpected_token_error(
	expected: &str,
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let value = fragment.value();
	let message = format!(
		"Unexpected token: expected {}, got {}",
		expected, value
	);
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
	}
}

/// Error for unsupported tokens
pub fn unsupported_token_error(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let value = fragment.value();
	let message = format!("Unsupported token: {}", value);
	let label = Some(format!("found `{}`", value));

	Diagnostic {
		code: "AST_006".to_string(),
		statement: None,
		message,
		column: None,
		fragment,
		label,
		help: Some("This token is not supported in this context"
			.to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Multiple expressions require curly braces
pub fn multiple_expressions_without_braces(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let keyword = fragment.value().to_string();
	Diagnostic {
		code: "AST_007".to_string(),
		statement: None,
		message: format!(
			"multiple expressions in `{}` require curly braces",
			&keyword
		),
		fragment,
		label: Some("missing `{ … }` around expressions".to_string()),
		help: Some(format!(
			"wrap the expressions in curly braces:\n    {} {{ expr1, expr2, … }}",
			keyword
		)),
		column: None,
		notes: vec![],
		cause: None,
	}
}

/// Type not found error
pub fn unrecognized_type(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let type_name = fragment.value().to_string();
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
	}
}
