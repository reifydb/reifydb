// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	fmt::{self, Display},
	ops::Deref,
};

use serde::{Deserialize, Serialize};

use crate::ast::{Ast, AstFrom, InfixOperator};

/// A transparent wrapper around String representing a query string
/// that can be parsed and executed
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryString(pub String);

impl QueryString {
	pub fn new(query: String) -> Self {
		QueryString(query)
	}

	pub fn from_ast(ast: &Ast) -> crate::Result<Self> {
		let query_str = reconstruct_query(ast)?;
		Ok(QueryString(query_str))
	}

	/// Get the query string as a &str
	pub fn as_str(&self) -> &str {
		&self.0
	}
}

impl Display for QueryString {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for QueryString {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<String> for QueryString {
	fn from(s: String) -> Self {
		QueryString(s)
	}
}

impl From<QueryString> for String {
	fn from(q: QueryString) -> Self {
		q.0
	}
}

impl AsRef<str> for QueryString {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

/// Reconstruct a query string from an AST node
fn reconstruct_query(ast: &Ast) -> crate::Result<String> {
	match ast {
		Ast::From(from_node) => match from_node {
			AstFrom::Source {
				source,
				..
			} => {
				// Build the source name from the identifier parts
				let source_name = if let Some(ns) = &source.namespace {
					format!("{}.{}", ns.text(), source.name.text())
				} else {
					source.name.text().to_string()
				};
				Ok(format!("from {}", source_name))
			}
			AstFrom::Inline {
				..
			} => {
				unimplemented!()
			}
		},
		Ast::Infix(infix) => {
			match &infix.operator {
				InfixOperator::AccessTable(_) => {
					// WORKAROUND: The join's 'with' field should be an AstStatement,
					// but currently it's just an Ast node representing the table reference.
					// We construct a "from" query here, but this is temporary until
					// the AST structure is fixed.
					let left_str = reconstruct_ast_as_string(&*infix.left)?;
					let right_str = reconstruct_ast_as_string(&*infix.right)?;
					Ok(format!("from {}.{}", left_str, right_str))
				}
				_ => {
					unimplemented!(
						"Query reconstruction not implemented for infix operator: {:?}",
						infix.operator
					)
				}
			}
		}
		Ast::Identifier(ident) => {
			// WORKAROUND: For simple identifiers (unqualified table names),
			// construct a "from" query. This should be an AstStatement instead.
			Ok(format!("from {}", ident.token.fragment.text()))
		}
		_ => {
			unimplemented!("Query reconstruction not implemented for this AST type: {:?}", ast)
		}
	}
}

/// Helper function to reconstruct a simple AST node as a string
fn reconstruct_ast_as_string(ast: &Ast) -> crate::Result<String> {
	match ast {
		Ast::Identifier(ident) => Ok(ident.token.fragment.text().to_string()),
		Ast::Infix(infix) => {
			use crate::ast::InfixOperator;
			match &infix.operator {
				InfixOperator::AccessTable(_) => {
					let left = reconstruct_ast_as_string(&*infix.left)?;
					let right = reconstruct_ast_as_string(&*infix.right)?;
					Ok(format!("{}.{}", left, right))
				}
				_ => {
					unimplemented!("Unsupported infix operator in identifier: {:?}", infix.operator)
				}
			}
		}
		_ => {
			unimplemented!("Cannot convert AST node to string: {:?}", ast)
		}
	}
}
