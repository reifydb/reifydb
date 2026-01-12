// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DROP statement parsing.
//!
//! RQL syntax: `DROP TABLE|VIEW|FLOW|INDEX|NAMESPACE [IF EXISTS] name [CASCADE|RESTRICT]`

use crate::{
	ast::{
		Statement,
		parse::{ParseError, ParseErrorKind, Parser},
		stmt::ddl::{DropObjectType, DropStmt},
	},
	token::Keyword,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse DROP statement.
	///
	/// Syntax: `DROP object_type [IF EXISTS] [namespace.]name [CASCADE|RESTRICT]`
	///
	/// # Examples
	///
	/// ```rql
	/// DROP TABLE test.users
	/// DROP FLOW IF EXISTS myflow
	/// DROP INDEX idx_email CASCADE
	/// DROP NAMESPACE mydb RESTRICT
	/// ```
	pub(in crate::ast::parse) fn parse_drop(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Drop)?;

		// Determine object type
		let object_type = if self.try_consume_keyword(Keyword::Table) {
			DropObjectType::Table
		} else if self.try_consume_keyword(Keyword::View) {
			DropObjectType::View
		} else if self.try_consume_keyword(Keyword::Flow) {
			DropObjectType::Flow
		} else if self.try_consume_keyword(Keyword::Index) {
			DropObjectType::Index
		} else if self.try_consume_keyword(Keyword::Namespace) {
			DropObjectType::Namespace
		} else if self.try_consume_keyword(Keyword::Sequence) {
			DropObjectType::Sequence
		} else if self.try_consume_keyword(Keyword::Dictionary) {
			DropObjectType::Dictionary
		} else if self.try_consume_keyword(Keyword::Ringbuffer) {
			DropObjectType::RingBuffer
		} else {
			return Err(self.error(ParseErrorKind::UnexpectedToken));
		};

		// Check for IF EXISTS
		let if_exists = self.try_parse_if_exists();

		// Parse name (potentially qualified: namespace.name)
		let (namespace, name) = self.parse_qualified_identifier()?;

		// Check for CASCADE or RESTRICT (optional)
		let _cascade = self.try_consume_keyword(Keyword::Cascade);
		let _restrict = self.try_consume_keyword(Keyword::Restrict);

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Drop(DropStmt::new(
			object_type,
			namespace,
			name,
			if_exists,
			span,
		)))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{Statement, stmt::ddl::DropObjectType},
		token::tokenize,
	};

	#[test]
	fn test_drop_table_simple() {
		let bump = Bump::new();
		let source = "DROP TABLE users";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Table);
				assert!(d.namespace.is_none());
				assert_eq!(d.name, "users");
				assert!(!d.if_exists);
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_table_qualified() {
		let bump = Bump::new();
		let source = "DROP TABLE test.users";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Table);
				assert_eq!(d.namespace, Some("test"));
				assert_eq!(d.name, "users");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_flow_if_exists() {
		let bump = Bump::new();
		let source = "DROP FLOW IF EXISTS myflow";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Flow);
				assert!(d.if_exists);
				assert_eq!(d.name, "myflow");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_namespace() {
		let bump = Bump::new();
		let source = "DROP NAMESPACE mydb";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Namespace);
				assert_eq!(d.name, "mydb");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_index_cascade() {
		let bump = Bump::new();
		let source = "DROP INDEX idx_email CASCADE";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Index);
				assert_eq!(d.name, "idx_email");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_lowercase() {
		let bump = Bump::new();
		let source = "drop view myview";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::View);
				assert_eq!(d.name, "myview");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_flow_basic() {
		let bump = Bump::new();
		let source = "DROP FLOW my_flow";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Flow);
				assert!(d.namespace.is_none());
				assert_eq!(d.name, "my_flow");
				assert!(!d.if_exists);
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_flow_qualified() {
		let bump = Bump::new();
		let source = "DROP FLOW analytics.sales_flow";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Flow);
				assert_eq!(d.namespace, Some("analytics"));
				assert_eq!(d.name, "sales_flow");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_flow_cascade() {
		let bump = Bump::new();
		let source = "DROP FLOW my_flow CASCADE";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Flow);
				assert_eq!(d.name, "my_flow");
				// Note: v2 parses CASCADE but doesn't store it in the struct
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_flow_restrict() {
		let bump = Bump::new();
		let source = "DROP FLOW my_flow RESTRICT";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Flow);
				assert_eq!(d.name, "my_flow");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_flow_if_exists_cascade() {
		let bump = Bump::new();
		let source = "DROP FLOW IF EXISTS test.my_flow CASCADE";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Flow);
				assert!(d.if_exists);
				assert_eq!(d.namespace, Some("test"));
				assert_eq!(d.name, "my_flow");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_sequence() {
		let bump = Bump::new();
		let source = "DROP SEQUENCE test.seq";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Sequence);
				assert_eq!(d.namespace, Some("test"));
				assert_eq!(d.name, "seq");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_dictionary() {
		let bump = Bump::new();
		let source = "DROP DICTIONARY my_dict";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::Dictionary);
				assert_eq!(d.name, "my_dict");
			}
			_ => panic!("Expected DROP statement"),
		}
	}

	#[test]
	fn test_drop_ringbuffer() {
		let bump = Bump::new();
		let source = "DROP RINGBUFFER test.events";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Drop(d) => {
				assert_eq!(d.object_type, DropObjectType::RingBuffer);
				assert_eq!(d.namespace, Some("test"));
				assert_eq!(d.name, "events");
			}
			_ => panic!("Expected DROP statement"),
		}
	}
}
