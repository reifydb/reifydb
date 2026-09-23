// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::value_type::ValueType,
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

	#[error("unsupported type parameters")]
	UnsupportedTypeParameters {
		fragment: Fragment,
	},

	#[error("unsupported query syntax: {node_type}")]
	UnsupportedAstNode {
		node_type: String,
		fragment: Fragment,
	},

	#[error("maximum nesting depth exceeded")]
	MaxDepthExceeded {
		fragment: Fragment,
	},

	#[error("digest not supported for {inner}")]
	DigestInputTypeUnsupported {
		inner: ValueType,
		fragment: Fragment,
	},

	#[error("accuracy must be between 0.001 and 0.1")]
	DigestAccuracyOutOfRange {
		fragment: Fragment,
	},

	#[error("accuracy must be a whole number of parts per million")]
	DigestAccuracyNotWholePpm {
		fragment: Fragment,
	},

	#[error("accuracy must be a number")]
	DigestAccuracyNotANumber {
		fragment: Fragment,
	},

	#[error("digest needs an input type and an accuracy")]
	DigestAccuracyMissing {
		fragment: Fragment,
	},

	#[error("digest takes an input type and an accuracy only")]
	DigestTooManyParameters {
		fragment: Fragment,
	},

	#[error("digest input must be a type")]
	DigestInputNotAType {
		fragment: Fragment,
	},

	#[error("Option cannot wrap another Option")]
	NestedOption {
		fragment: Fragment,
	},

	#[error("list needs an item type")]
	ListItemMissing {
		fragment: Fragment,
	},

	#[error("list takes one item type only")]
	ListTooManyParameters {
		fragment: Fragment,
	},

	#[error("list item must be a type")]
	ListItemNotAType {
		fragment: Fragment,
	},

	#[error("list item must be a scalar type")]
	ListItemNotScalar {
		fragment: Fragment,
	},

	#[error("list item cannot carry a constraint")]
	ListItemConstrained {
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
				rql: None,
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
				rql: None,
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
					rql: None,
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
					rql: None,
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
					rql: None,
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
					rql: None,
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
					rql: None,
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
			AstError::UnsupportedTypeParameters {
				fragment,
			} => {
				let type_name = fragment.text().to_string();
				Diagnostic {
					code: "AST_012".to_string(),
					rql: None,
					message: format!("type `{}` does not accept these parameters", &type_name),
					fragment,
					label: Some("unsupported type parameters".to_string()),
					help: Some("Only utf8 and blob take a byte limit, e.g. utf8(255), int and uint take a precision, e.g. int(20), decimal takes precision and scale, e.g. decimal(10,2), and digest takes an input type and an accuracy, e.g. digest(float8, 0.01)".to_string()),
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
				rql: None,
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
			AstError::MaxDepthExceeded {
				fragment,
			} => Diagnostic {
				code: "AST_010".to_string(),
				rql: None,
				message: "maximum nesting depth exceeded".to_string(),
				fragment,
				label: Some("expression is too deeply nested".to_string()),
				help: Some("Reduce the nesting depth of your expression".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::DigestInputTypeUnsupported {
				inner,
				fragment,
			} => Diagnostic {
				code: "AST_013".to_string(),
				rql: None,
				message: format!("digest not supported for {}", inner),
				fragment,
				label: Some("unsupported digest input type".to_string()),
				help: Some("A digest takes an int, uint, float or duration input, e.g. digest(float8, 0.01)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::DigestAccuracyOutOfRange {
				fragment,
			} => Diagnostic {
				code: "AST_014".to_string(),
				rql: None,
				message: "accuracy must be between 0.001 and 0.1".to_string(),
				fragment,
				label: Some("accuracy out of range".to_string()),
				help: Some("Write an accuracy from 0.001 to 0.1, e.g. digest(float8, 0.01)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::DigestAccuracyNotWholePpm {
				fragment,
			} => Diagnostic {
				code: "AST_015".to_string(),
				rql: None,
				message: "accuracy must be a whole number of parts per million".to_string(),
				fragment,
				label: Some("accuracy has too many decimal places".to_string()),
				help: Some("Write the accuracy with at most six decimal places, e.g. digest(float8, 0.0125)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::DigestAccuracyNotANumber {
				fragment,
			} => Diagnostic {
				code: "AST_016".to_string(),
				rql: None,
				message: "accuracy must be a number".to_string(),
				fragment,
				label: Some("not a number".to_string()),
				help: Some("Write the accuracy as a number literal, e.g. digest(float8, 0.01)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::DigestAccuracyMissing {
				fragment,
			} => Diagnostic {
				code: "AST_017".to_string(),
				rql: None,
				message: "digest needs an input type and an accuracy".to_string(),
				fragment,
				label: Some("accuracy missing".to_string()),
				help: Some("Write digest(<input type>, <accuracy>), e.g. digest(float8, 0.01)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::DigestTooManyParameters {
				fragment,
			} => Diagnostic {
				code: "AST_018".to_string(),
				rql: None,
				message: "digest takes an input type and an accuracy only".to_string(),
				fragment,
				label: Some("unexpected parameter".to_string()),
				help: Some("Remove the extra parameter, e.g. digest(float8, 0.01)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::DigestInputNotAType {
				fragment,
			} => Diagnostic {
				code: "AST_019".to_string(),
				rql: None,
				message: "digest input must be a type".to_string(),
				fragment,
				label: Some("not a type".to_string()),
				help: Some("Write the input type first, e.g. digest(float8, 0.01)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::NestedOption {
				fragment,
			} => Diagnostic {
				code: "AST_020".to_string(),
				rql: None,
				message: "Option cannot wrap another Option".to_string(),
				fragment,
				label: Some("already optional".to_string()),
				help: Some("Write a single Option, e.g. Option(int4); none is the only missing value".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::ListItemMissing {
				fragment,
			} => Diagnostic {
				code: "AST_021".to_string(),
				rql: None,
				message: "list needs an item type".to_string(),
				fragment,
				label: Some("item type missing".to_string()),
				help: Some("Write list(<item type>), e.g. list(uuid7)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::ListTooManyParameters {
				fragment,
			} => Diagnostic {
				code: "AST_022".to_string(),
				rql: None,
				message: "list takes one item type only".to_string(),
				fragment,
				label: Some("unexpected parameter".to_string()),
				help: Some("Remove the extra parameter, e.g. list(uuid7)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::ListItemNotAType {
				fragment,
			} => Diagnostic {
				code: "AST_023".to_string(),
				rql: None,
				message: "list item must be a type".to_string(),
				fragment,
				label: Some("not a type".to_string()),
				help: Some("Write the item type, e.g. list(uuid7)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::ListItemNotScalar {
				fragment,
			} => Diagnostic {
				code: "AST_024".to_string(),
				rql: None,
				message: "list item must be a scalar type".to_string(),
				fragment,
				label: Some("not a scalar".to_string()),
				help: Some("Use a scalar item type, e.g. list(uuid7)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			AstError::ListItemConstrained {
				fragment,
			} => Diagnostic {
				code: "AST_025".to_string(),
				rql: None,
				message: "list item cannot carry a constraint".to_string(),
				fragment,
				label: Some("constraint not kept".to_string()),
				help: Some("Drop the item's parameters, e.g. list(utf8)".to_string()),
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
		Error(Box::new(err.into_diagnostic()))
	}
}
