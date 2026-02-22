// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, thiserror::Error)]
pub enum AstError {
	#[error("tokenizer error: {message}")]
	TokenizeError {
		message: String,
	},

	#[error("Unexpected end of file")]
	UnexpectedEof,

	#[error("unexpected token: expected `identifier`")]
	ExpectedIdentifier {
		fragment: Fragment,
	},

	#[error("Unexpected token: expected {expected}")]
	UnexpectedToken {
		expected: String,
		fragment: Fragment,
	},

	#[error("Unsupported token")]
	UnsupportedToken {
		fragment: Fragment,
	},

	#[error("multiple expressions require curly braces")]
	MultipleExpressionsWithoutBraces {
		fragment: Fragment,
	},

	#[error("cannot find type")]
	UnrecognizedType {
		fragment: Fragment,
	},

	#[error("unsupported query syntax: {node_type}")]
	UnsupportedAstNode {
		node_type: String,
		fragment: Fragment,
	},
}

impl IntoDiagnostic for AstError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			AstError::TokenizeError {
				message,
			} => Diagnostic {
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
			},
			AstError::UnexpectedEof => Diagnostic {
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
			},
			AstError::ExpectedIdentifier {
				fragment,
			} => {
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
			AstError::UnexpectedToken {
				expected,
				fragment,
			} => {
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
			AstError::UnsupportedToken {
				fragment,
			} => {
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
			AstError::MultipleExpressionsWithoutBraces {
				fragment,
			} => {
				let keyword = fragment.text().to_string();
				Diagnostic {
					code: "AST_007".to_string(),
					statement: None,
					message: format!("multiple expressions in `{}` require curly braces", &keyword),
					fragment,
					label: Some("missing `{ … }` around expressions".to_string()),
					help: Some(format!(
						"wrap the expressions in curly braces:\n    {} {{ expr1, expr2, … }}",
						keyword
					)),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			AstError::UnrecognizedType {
				fragment,
			} => {
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
			AstError::UnsupportedAstNode {
				node_type,
				fragment,
			} => Diagnostic {
				code: "AST_009".to_string(),
				statement: None,
				message: format!("unsupported query syntax: {}", node_type),
				fragment,
				label: Some("not supported in this context".to_string()),
				help: Some("This syntax is not yet supported or may be invalid in this context"
					.to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<AstError> for Error {
	fn from(err: AstError) -> Self {
		Error(err.into_diagnostic())
	}
}
