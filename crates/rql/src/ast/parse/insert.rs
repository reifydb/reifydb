// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::{insert_missing_from_clause, insert_missing_target};
use reifydb_type::return_error;

use crate::ast::{
	ast::{Ast, AstInsert},
	identifier::UnresolvedPrimitiveIdentifier,
	parse::Parser,
	tokenize::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl Parser {
	/// Parse INSERT statement with keyword-first syntax:
	/// INSERT table FROM [...]
	/// INSERT namespace.table FROM source_table
	/// INSERT table FROM $variable
	pub(crate) fn parse_insert(&mut self) -> crate::Result<AstInsert> {
		let token = self.consume_keyword(Keyword::Insert)?;

		// 1. Parse target (REQUIRED) - namespace.table or just table
		if self.is_eof() || !matches!(self.current()?.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			return_error!(insert_missing_target(token.fragment));
		}

		let first = self.parse_identifier_with_hyphens()?;
		let target = if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let second = self.parse_identifier_with_hyphens()?;
			// namespace.table
			UnresolvedPrimitiveIdentifier::new(Some(first.into_fragment()), second.into_fragment())
		} else {
			// table only
			UnresolvedPrimitiveIdentifier::new(None, first.into_fragment())
		};

		// 2. Parse FROM clause (REQUIRED)
		if self.is_eof() || !self.current()?.is_keyword(Keyword::From) {
			return_error!(insert_missing_from_clause(token.fragment));
		}

		// Parse the FROM clause which handles inline arrays, variables, and table sources
		let source = Ast::From(self.parse_from()?);

		Ok(AstInsert {
			token,
			target,
			source: Box::new(source),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::ast::{
		ast::{Ast, AstFrom, AstInsert},
		parse::Parser,
		tokenize::tokenize,
	};

	#[test]
	fn test_insert_with_inline_array() {
		let tokens = tokenize(
			r#"
        INSERT users FROM [{ id: 1, name: "Alice" }]
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target
		assert!(insert.target.namespace.is_none());
		assert_eq!(insert.target.name.text(), "users");

		// Check source is FROM with inline data
		assert!(matches!(*insert.source, Ast::From(AstFrom::Inline { .. })));
	}

	#[test]
	fn test_insert_with_namespace() {
		let tokens = tokenize(
			r#"
        INSERT test.users FROM [{ id: 1, name: "Bob" }]
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target with namespace
		assert_eq!(insert.target.namespace.as_ref().unwrap().text(), "test");
		assert_eq!(insert.target.name.text(), "users");
	}

	#[test]
	fn test_insert_from_source_table() {
		let tokens = tokenize(
			r#"
        INSERT target_table FROM source_table
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target
		assert!(insert.target.namespace.is_none());
		assert_eq!(insert.target.name.text(), "target_table");

		// Check source is FROM with table source
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = &*insert.source
		{
			assert_eq!(source.name.text(), "source_table");
		} else {
			panic!("Expected FROM with table source");
		}
	}

	#[test]
	fn test_insert_from_variable() {
		let tokens = tokenize(
			r#"
        INSERT users FROM $data
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target
		assert!(insert.target.namespace.is_none());
		assert_eq!(insert.target.name.text(), "users");

		// Check source is FROM with variable
		assert!(matches!(*insert.source, Ast::From(AstFrom::Variable { .. })));
	}

	#[test]
	fn test_insert_missing_from_fails() {
		let tokens = tokenize(
			r#"
        INSERT users
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_insert_missing_target_fails() {
		let tokens = tokenize(
			r#"
        INSERT FROM [{ id: 1 }]
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_insert_multiple_rows() {
		let tokens = tokenize(
			r#"
        INSERT users FROM [
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
          { id: 3, name: "Charlie" }
        ]
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check source has 3 rows
		if let Ast::From(AstFrom::Inline {
			list,
			..
		}) = &*insert.source
		{
			assert_eq!(list.len(), 3);
		} else {
			panic!("Expected FROM with inline data");
		}
	}
}
