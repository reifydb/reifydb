// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::{insert_missing_source, insert_missing_target};
use reifydb_type::return_error;

use crate::{
	ast::{
		ast::{Ast, AstFrom, AstInsert, AstVariable},
		identifier::UnresolvedPrimitiveIdentifier,
		parse::Parser,
	},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse INSERT statement with keyword-first syntax:
	/// INSERT table [...]              - inline array (no FROM)
	/// INSERT table $variable          - variable (no FROM)
	/// INSERT namespace.table FROM source_table  - table source (FROM required)
	pub(crate) fn parse_insert(&mut self) -> crate::Result<AstInsert<'bump>> {
		let token = self.consume_keyword(Keyword::Insert)?;

		// 1. Parse target (REQUIRED) - namespace.table or just table
		if self.is_eof() || !matches!(self.current()?.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			return_error!(insert_missing_target(token.fragment.to_owned()));
		}

		let mut segments = self.parse_dot_separated_identifiers()?;
		let target = if segments.len() > 1 {
			let name = segments.pop().unwrap().into_fragment();
			let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
			UnresolvedPrimitiveIdentifier::new(namespace, name)
		} else {
			UnresolvedPrimitiveIdentifier::new(vec![], segments.remove(0).into_fragment())
		};

		// 2. Parse data source
		// Check what follows the target:
		// - `[` → inline array (no FROM keyword)
		// - `$` → variable (no FROM keyword)
		// - `FROM` keyword → table/generator source (parse FROM clause)
		if self.is_eof() {
			return_error!(insert_missing_source(token.fragment.to_owned()));
		}

		let current = self.current()?;
		let source = if current.is_operator(Operator::OpenBracket) {
			// Inline array - parse directly without FROM keyword
			// Reuse parse_static from from.rs
			let list = self.parse_static()?;
			Ast::From(AstFrom::Inline {
				token: list.token,
				list,
			})
		} else if matches!(current.kind, TokenKind::Variable) {
			// Variable - parse directly without FROM keyword
			let var_token = self.advance()?;

			if var_token.fragment.text() == "$env" {
				Ast::From(AstFrom::Environment {
					token: var_token,
				})
			} else {
				let variable = AstVariable {
					token: var_token,
				};
				Ast::From(AstFrom::Variable {
					token: var_token,
					variable,
				})
			}
		} else if current.is_keyword(Keyword::From) {
			// Table/generator source - use FROM clause
			Ast::From(self.parse_from()?)
		} else {
			return_error!(insert_missing_source(token.fragment.to_owned()));
		};

		Ok(AstInsert {
			token,
			target,
			source: BumpBox::new_in(source, self.bump()),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstFrom},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_insert_with_inline_array() {
		let bump = Bump::new();
		// New syntax: no FROM keyword for inline arrays
		let tokens = tokenize(
			&bump,
			r#"
        INSERT users [{ id: 1, name: "Alice" }]
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target
		assert!(insert.target.namespace.is_empty());
		assert_eq!(insert.target.name.text(), "users");

		// Check source is FROM with inline data
		assert!(matches!(*insert.source, Ast::From(AstFrom::Inline { .. })));
	}

	#[test]
	fn test_insert_with_namespace() {
		let bump = Bump::new();
		// New syntax: no FROM keyword for inline arrays
		let tokens = tokenize(
			&bump,
			r#"
        INSERT test.users [{ id: 1, name: "Bob" }]
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target with namespace
		assert_eq!(insert.target.namespace[0].text(), "test");
		assert_eq!(insert.target.name.text(), "users");
	}

	#[test]
	fn test_insert_from_source_table() {
		let bump = Bump::new();
		// Table sources still use FROM keyword
		let tokens = tokenize(
			&bump,
			r#"
        INSERT target_table FROM source_table
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target
		assert!(insert.target.namespace.is_empty());
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
	fn test_insert_variable() {
		let bump = Bump::new();
		// New syntax: no FROM keyword for variables
		let tokens = tokenize(
			&bump,
			r#"
        INSERT users $data
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		// Check target
		assert!(insert.target.namespace.is_empty());
		assert_eq!(insert.target.name.text(), "users");

		// Check source is FROM with variable
		assert!(matches!(*insert.source, Ast::From(AstFrom::Variable { .. })));
	}

	#[test]
	fn test_insert_missing_source_fails() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        INSERT users
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_insert_missing_target_fails() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        INSERT [{ id: 1 }]
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_insert_multiple_rows() {
		let bump = Bump::new();
		// New syntax: no FROM keyword for inline arrays
		let tokens = tokenize(
			&bump,
			r#"
        INSERT users [
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
          { id: 3, name: "Charlie" }
        ]
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
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

	#[test]
	fn test_insert_positional_single_row() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"INSERT users [(1, "Alice")]"#).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		assert!(insert.target.namespace.is_empty());
		assert_eq!(insert.target.name.text(), "users");

		if let Ast::From(AstFrom::Inline {
			list,
			..
		}) = &*insert.source
		{
			assert_eq!(list.len(), 1);
			let tuple = list[0].as_tuple();
			assert_eq!(tuple.len(), 2);
		} else {
			panic!("Expected FROM with inline data");
		}
	}

	#[test]
	fn test_insert_positional_multiple_rows() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
			INSERT users [
			  (1, "Alice", "alice@example.com", true),
			  (2, "Bob", "bob@example.com", false)
			]
			"#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		if let Ast::From(AstFrom::Inline {
			list,
			..
		}) = &*insert.source
		{
			assert_eq!(list.len(), 2);
			assert_eq!(list[0].as_tuple().len(), 4);
			assert_eq!(list[1].as_tuple().len(), 4);
		} else {
			panic!("Expected FROM with inline data");
		}
	}

	#[test]
	fn test_insert_positional_with_namespace() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"INSERT test.users [(1, "Alice")]"#).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		assert_eq!(insert.target.namespace[0].text(), "test");
		assert_eq!(insert.target.name.text(), "users");

		if let Ast::From(AstFrom::Inline {
			list,
			..
		}) = &*insert.source
		{
			assert_eq!(list.len(), 1);
			assert!(matches!(list[0], Ast::Tuple(_)));
		} else {
			panic!("Expected FROM with inline data");
		}
	}
}
