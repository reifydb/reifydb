// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
};

use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::value_type::ValueType,
};

#[derive(Debug, Clone, PartialEq)]
pub enum OperationKind {
	Sort,
	Map,
	Extend,
	Patch,
	Apply,
	Aggregate,
	AggregateBy,
	Returning,
}

impl Display for OperationKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			OperationKind::Sort => f.write_str("SORT"),
			OperationKind::Map => f.write_str("MAP"),
			OperationKind::Extend => f.write_str("EXTEND"),
			OperationKind::Patch => f.write_str("PATCH"),
			OperationKind::Apply => f.write_str("APPLY"),
			OperationKind::Aggregate => f.write_str("AGGREGATE"),
			OperationKind::AggregateBy => f.write_str("AGGREGATE BY"),
			OperationKind::Returning => f.write_str("RETURNING"),
		}
	}
}

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

	#[error("TAKE operator requires non-negative value")]
	TakeNegativeValue {
		fragment: Fragment,
	},

	#[error("{kind} requires curly braces")]
	OperatorMissingBraces {
		kind: OperationKind,
		fragment: Fragment,
	},

	#[error("WITH key `{key}` is given more than once")]
	OperatorWithDuplicateKey {
		key: String,
		fragment: Fragment,
	},

	#[error("UPDATE requires an assignments block")]
	UpdateMissingAssignmentsBlock {
		fragment: Fragment,
	},

	#[error("UPDATE assignments block cannot be empty")]
	UpdateEmptyAssignmentsBlock {
		fragment: Fragment,
	},

	#[error("UPDATE requires a FILTER clause")]
	UpdateMissingFilterClause {
		fragment: Fragment,
	},

	#[error("DELETE requires a target table")]
	DeleteMissingTarget {
		fragment: Fragment,
	},

	#[error("DELETE requires a FILTER clause")]
	DeleteMissingFilterClause {
		fragment: Fragment,
	},

	#[error("INSERT requires a target table")]
	InsertMissingTarget {
		fragment: Fragment,
	},

	#[error("INSERT requires a data source")]
	InsertMissingSource {
		fragment: Fragment,
	},

	#[error("unknown INSERT WITH option `{option}`")]
	InsertWithUnknownOption {
		fragment: Fragment,
		option: String,
	},

	#[error("duplicate INSERT WITH option `{option}`")]
	InsertWithDuplicateOption {
		fragment: Fragment,
		option: String,
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
					help: Some("Only utf8 and blob take a byte limit, e.g. utf8(255), decimal takes precision and scale, e.g. decimal(10,2), and digest takes an input type and an accuracy, e.g. digest(float8, 0.01)".to_string()),
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
			AstError::TakeNegativeValue { fragment } => {
				let value = fragment.text().to_string();
				Diagnostic {
					code: "TAKE_001".to_string(),
					rql: None,
					message: format!("TAKE operator requires non-negative value, got {}", value),
					column: None,
					fragment,
					label: Some("negative value not allowed".to_string()),
					help: Some("Use a positive number to limit results, or 0 to return no rows".to_string()),
					notes: vec![
						"TAKE operator limits the number of rows returned from a query".to_string(),
						"Negative values are not meaningful in this context".to_string(),
						"Valid examples: TAKE 10, TAKE 0, TAKE 100".to_string(),
					],
					cause: None,
					operator_chain: None,
				}
			}



			AstError::OperatorMissingBraces { kind, fragment } => {
				let (code, message, help, notes) = match kind {
					OperationKind::Sort => (
						"SORT_001",
						"SORT requires curly braces around columns",
						"Wrap SORT columns in curly braces, e.g., 'SORT {name}' or 'SORT {name:ASC, age:DESC}'",
						vec![
							"SORT always requires curly braces: SORT {column}".to_string(),
							"Multiple columns: SORT {col1:ASC, col2:DESC}".to_string(),
						],
					),
					OperationKind::Map => (
						"MAP_002",
						"MAP requires curly braces around expressions",
						"Wrap MAP expressions in curly braces, e.g., 'MAP {name}' or 'MAP {name, age}'",
						vec![
							"MAP always requires curly braces: MAP {expression}".to_string(),
							"Multiple expressions: MAP {expr1, expr2}".to_string(),
						],
					),
					OperationKind::Extend => (
						"EXTEND_003",
						"EXTEND requires curly braces around expressions",
						"Wrap EXTEND expressions in curly braces, e.g., 'EXTEND {total: price * qty}'",
						vec![
							"EXTEND always requires curly braces: EXTEND {expression}".to_string(),
							"Multiple expressions: EXTEND {expr1, expr2}".to_string(),
						],
					),
					OperationKind::Patch => (
						"PATCH_001",
						"PATCH requires curly braces around expressions",
						"Wrap PATCH expressions in curly braces, e.g., 'PATCH {status: \"active\"}'",
						vec![
							"PATCH always requires curly braces: PATCH {expression}".to_string(),
							"Multiple expressions: PATCH {expr1, expr2}".to_string(),
						],
					),
					OperationKind::Apply => (
						"APPLY_002",
						"APPLY requires curly braces around arguments",
						"Wrap APPLY arguments in curly braces, e.g., 'APPLY running_sum {value}'",
						vec![
							"APPLY always requires curly braces: APPLY operator {arguments}".to_string(),
							"No arguments: APPLY counter {}".to_string(),
							"Multiple arguments: APPLY op {arg1, arg2}".to_string(),
						],
					),
					OperationKind::Aggregate => (
						"AGGREGATE_004",
						"AGGREGATE requires curly braces around aggregation expressions",
						"Wrap aggregation expressions in curly braces, e.g., 'AGGREGATE {count(id)} BY {name}'",
						vec![
							"AGGREGATE always requires curly braces: AGGREGATE {expressions} BY {columns}".to_string(),
							"Example: AGGREGATE {sum(amount), avg(price)} BY {category}".to_string(),
						],
					),
					OperationKind::AggregateBy => (
						"AGGREGATE_005",
						"AGGREGATE BY requires curly braces around grouping columns",
						"Wrap BY columns in curly braces, e.g., 'AGGREGATE {count(id)} BY {name}'",
						vec![
							"AGGREGATE BY always requires curly braces: BY {columns}".to_string(),
							"Global aggregation (no grouping): BY {}".to_string(),
							"Multiple columns: BY {col1, col2}".to_string(),
						],
					),
					OperationKind::Returning => (
						"RETURNING_001",
						"RETURNING requires curly braces around expressions",
						"Wrap RETURNING expressions in curly braces, e.g., 'RETURNING {id, name}'",
						vec![
							"RETURNING always requires curly braces: RETURNING {expressions}".to_string(),
							"All columns: RETURNING {*}".to_string(),
							"Computed: RETURNING {id, total: price * qty}".to_string(),
						],
					),
				};
				Diagnostic {
					code: code.to_string(),
					rql: None,
					message: message.to_string(),
					column: None,
					fragment,
					label: Some("missing curly braces".to_string()),
					help: Some(help.to_string()),
					notes,
					cause: None,
					operator_chain: None,
				}
			}

			AstError::OperatorWithDuplicateKey { key, fragment } => Diagnostic {
				code: "OP_003".to_string(),
				rql: None,
				message: format!("WITH key `{}` is given more than once", key),
				column: None,
				fragment,
				label: Some("key given twice".to_string()),
				help: Some("Each WITH key may appear at most once in its block".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AstError::UpdateMissingAssignmentsBlock { fragment } => Diagnostic {
				code: "UPDATE_001".to_string(),
				rql: None,
				message: "UPDATE requires an assignments block".to_string(),
				column: None,
				fragment,
				label: Some("missing assignments block".to_string()),
				help: Some(
					"Specify the fields to update in curly braces, e.g., 'UPDATE users { name: 'alice' } FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"The assignments block specifies which columns to update and their new values".to_string(),
					"Use colon syntax for assignments: { column: value, ... }".to_string(),
					"Example: UPDATE users { name: 'alice', age: 30 } FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::UpdateEmptyAssignmentsBlock { fragment } => Diagnostic {
				code: "UPDATE_002".to_string(),
				rql: None,
				message: "UPDATE assignments block cannot be empty".to_string(),
				column: None,
				fragment,
				label: Some("empty assignments block".to_string()),
				help: Some(
					"Specify at least one field to update, e.g., 'UPDATE users { name: 'alice' } FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"At least one column must be specified for update".to_string(),
					"Example: UPDATE users { name: 'alice', age: 30 } FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::UpdateMissingFilterClause { fragment } => Diagnostic {
				code: "UPDATE_003".to_string(),
				rql: None,
				message: "UPDATE requires a FILTER clause".to_string(),
				column: None,
				fragment,
				label: Some("missing FILTER clause".to_string()),
				help: Some(
					"Add a FILTER clause to specify which rows to update, e.g., 'UPDATE users { name: 'alice' } FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"The FILTER clause is required to prevent accidental mass updates".to_string(),
					"It specifies the condition for selecting rows to update".to_string(),
					"Example: UPDATE users { name: 'alice' } FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::DeleteMissingTarget { fragment } => Diagnostic {
				code: "DELETE_001".to_string(),
				rql: None,
				message: "DELETE requires a target table".to_string(),
				column: None,
				fragment,
				label: Some("missing target table".to_string()),
				help: Some("Specify the table to delete from, e.g., 'DELETE users FILTER id == 1'".to_string()),
				notes: vec![
					"The target table specifies which table to delete rows from".to_string(),
					"Example: DELETE users FILTER id == 1".to_string(),
					"Example with namespace: DELETE test::users FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::DeleteMissingFilterClause { fragment } => Diagnostic {
				code: "DELETE_002".to_string(),
				rql: None,
				message: "DELETE requires a FILTER clause".to_string(),
				column: None,
				fragment,
				label: Some("missing FILTER clause".to_string()),
				help: Some(
					"Add a FILTER clause to specify which rows to delete, e.g., 'DELETE users FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"The FILTER clause is required to prevent accidental mass deletes".to_string(),
					"It specifies the condition for selecting rows to delete".to_string(),
					"Example: DELETE users FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::InsertMissingTarget { fragment } => Diagnostic {
				code: "INSERT_001".to_string(),
				rql: None,
				message: "INSERT requires a target table".to_string(),
				column: None,
				fragment,
				label: Some("missing target table".to_string()),
				help: Some(
					"Specify the table to insert into, e.g., 'INSERT users [{ id: 1, name: \"Alice\" }]'"
						.to_string(),
				),
				notes: vec![
					"The target table specifies where to insert the data".to_string(),
					"Example: INSERT users [{ id: 1, name: \"Alice\" }]".to_string(),
					"Example with namespace: INSERT test::users [{ id: 1, name: \"Alice\" }]".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::InsertMissingSource { fragment } => Diagnostic {
				code: "INSERT_002".to_string(),
				rql: None,
				message: "INSERT requires a data source".to_string(),
				column: None,
				fragment,
				label: Some("missing data source".to_string()),
				help: Some(
					"Specify the data to insert, e.g., 'INSERT users [{ id: 1, name: \"Alice\" }]'".to_string(),
				),
				notes: vec![
					"Use inline array directly: INSERT users [{ id: 1 }, { id: 2 }]".to_string(),
					"Use variable directly: INSERT users $data".to_string(),
					"Use FROM for table sources: INSERT target_table FROM source_table".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::InsertWithUnknownOption { fragment, option } => Diagnostic {
				code: "INSERT_006".to_string(),
				rql: None,
				message: format!("unknown INSERT WITH option `{}`", option),
				column: None,
				fragment,
				label: Some("unknown option".to_string()),
				help: Some("INSERT ... WITH accepts deduplication_key and not_before".to_string()),
				notes: vec![
					"Example: INSERT ns::jobs [{ id: 1 }] WITH { deduplication_key: \"job-1\" }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			AstError::InsertWithDuplicateOption { fragment, option } => Diagnostic {
				code: "INSERT_007".to_string(),
				rql: None,
				message: format!("duplicate INSERT WITH option `{}`", option),
				column: None,
				fragment,
				label: Some("option given twice".to_string()),
				help: Some("Each INSERT WITH option may appear at most once".to_string()),
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
