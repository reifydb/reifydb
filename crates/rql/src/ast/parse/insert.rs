// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstFrom, AstInsert, AstInsertWith, AstVariable},
		identifier::UnresolvedObjectIdentifier,
		parse::Parser,
	},
	bump::BumpBox,
	error::{OperationKind, RqlError},
	token::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_insert(&mut self) -> Result<AstInsert<'bump>> {
		let token = self.consume_keyword(Keyword::Insert)?;

		if self.is_eof() || !matches!(self.current()?.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			return Err(RqlError::InsertMissingTarget {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let target = if segments.len() > 1 {
			let name = segments.pop().unwrap().into_fragment();
			let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
			UnresolvedObjectIdentifier::new(namespace, name)
		} else {
			UnresolvedObjectIdentifier::new(vec![], segments.remove(0).into_fragment())
		};

		if self.is_eof() {
			return Err(RqlError::InsertMissingSource {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		let current = self.current()?;
		let source = if current.is_operator(Operator::OpenBracket) {
			let list = self.parse_static()?;
			Ast::From(AstFrom::Inline {
				token: list.token,
				list,
			})
		} else if matches!(current.kind, TokenKind::Variable) {
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
			Ast::From(self.parse_from()?)
		} else {
			return Err(RqlError::InsertMissingSource {
				fragment: token.fragment.to_owned(),
			}
			.into());
		};

		let with_options = if !self.is_eof() && self.current()?.is_keyword(Keyword::With) {
			Some(self.parse_insert_with()?)
		} else {
			None
		};

		let returning = if !self.is_eof() && self.current()?.is_keyword(Keyword::Returning) {
			let returning_token = self.advance()?;
			let (exprs, had_braces) = self.parse_expressions(true, false, None)?;
			if !had_braces {
				return Err(RqlError::OperatorMissingBraces {
					kind: OperationKind::Returning,
					fragment: returning_token.fragment.to_owned(),
				}
				.into());
			}
			Some(exprs)
		} else {
			None
		};

		Ok(AstInsert {
			token,
			target,
			source: BumpBox::new_in(source, self.bump()),
			with_options,
			returning,
		})
	}

	fn parse_insert_with(&mut self) -> Result<AstInsertWith<'bump>> {
		let token = self.consume_keyword(Keyword::With)?;
		let inline = self.parse_inline()?;

		let mut idempotency_key = None;
		let mut not_before = None;

		for keyed in inline.keyed_values {
			let name = keyed.key.text().to_string();
			let slot = match name.as_str() {
				"idempotency_key" => &mut idempotency_key,
				"not_before" => &mut not_before,
				_ => {
					return Err(RqlError::InsertWithUnknownOption {
						fragment: keyed.key.token.fragment.to_owned(),
						option: name,
					}
					.into());
				}
			};
			if slot.is_some() {
				return Err(RqlError::InsertWithDuplicateOption {
					fragment: keyed.key.token.fragment.to_owned(),
					option: name,
				}
				.into());
			}
			*slot = Some(keyed.value);
		}

		Ok(AstInsertWith {
			token,
			idempotency_key,
			not_before,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstFrom, AstStatement},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_insert_with_inline_array() {
		let bump = Bump::new();
		// New syntax: no FROM keyword for inline arrays
		let source = r#"
        INSERT users [{ id: 1, name: "Alice" }]
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"
        INSERT test::users [{ id: 1, name: "Bob" }]
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"
        INSERT target_table FROM source_table
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"
        INSERT users $data
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"
        INSERT users
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_insert_missing_target_fails() {
		let bump = Bump::new();
		let source = r#"
        INSERT [{ id: 1 }]
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_insert_multiple_rows() {
		let bump = Bump::new();
		// New syntax: no FROM keyword for inline arrays
		let source = r#"
        INSERT users [
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
          { id: 3, name: "Charlie" }
        ]
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"INSERT users [(1, "Alice")]"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"
			INSERT users [
			  (1, "Alice", "alice@example.com", true),
			  (2, "Bob", "bob@example.com", false)
			]
			"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"INSERT test::users [(1, "Alice")]"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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

	fn parse_one_statement<'b>(bump: &'b Bump, source: &'b str) -> AstStatement<'b> {
		let tokens = tokenize(bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(bump, source, tokens);
		let mut result = parser.parse().unwrap();
		result.pop().unwrap()
	}

	fn parse_insert_error(source: &str) -> String {
		let bump = Bump::new();
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		format!("{:?}", parser.parse().unwrap_err())
	}

	/// Both options must reach the plan. Dropping one silently would remove either
	/// the dedup guarantee or the delay the caller asked for.
	#[test]
	fn test_insert_with_parses_both_options() {
		let bump = Bump::new();
		let statement = parse_one_statement(
			&bump,
			r#"INSERT test::jobs [{ id: 1 }] WITH { idempotency_key: "k", not_before: n }"#,
		);
		let insert = statement.first_unchecked().as_insert();

		let with_options = insert.with_options.as_ref().expect("WITH must be parsed");
		assert!(with_options.idempotency_key.is_some());
		assert!(with_options.not_before.is_some());
	}

	/// Each option stands alone, so a caller may ask for dedup without a delay.
	#[test]
	fn test_insert_with_parses_a_single_option() {
		let bump = Bump::new();
		let statement =
			parse_one_statement(&bump, r#"INSERT test::jobs [{ id: 1 }] WITH { idempotency_key: "k" }"#);
		let insert = statement.first_unchecked().as_insert();

		let with_options = insert.with_options.as_ref().expect("WITH must be parsed");
		assert!(with_options.idempotency_key.is_some());
		assert!(with_options.not_before.is_none(), "an absent option must stay absent");
	}

	/// An INSERT with no WITH must not fabricate one, or every plain insert would
	/// take the desugar path and grow hidden columns it never asked for.
	#[test]
	fn test_insert_without_with_has_no_options() {
		let bump = Bump::new();
		let statement = parse_one_statement(&bump, "INSERT test::jobs [{ id: 1 }]");
		let insert = statement.first_unchecked().as_insert();

		assert!(insert.with_options.is_none());
	}

	/// WITH sits between the source and RETURNING. If the pipeline parser ever
	/// consumed the trailing WITH greedily, this ordering would stop parsing.
	#[test]
	fn test_insert_with_precedes_returning() {
		let bump = Bump::new();
		let statement = parse_one_statement(
			&bump,
			r#"INSERT test::jobs [{ id: 1 }] WITH { idempotency_key: "k" } RETURNING { id }"#,
		);
		let insert = statement.first_unchecked().as_insert();

		assert!(insert.with_options.is_some(), "WITH must survive a following RETURNING");
		assert!(insert.returning.is_some(), "RETURNING must survive a preceding WITH");
	}

	/// A misspelled option is a silently lost guarantee unless the parser rejects it.
	#[test]
	fn test_insert_with_rejects_an_unknown_option() {
		let error = parse_insert_error(r#"INSERT test::jobs [{ id: 1 }] WITH { nope: 1 }"#);
		assert!(error.contains("nope"), "the error must name the offending option, got: {error}");
	}

	/// A repeated option is ambiguous, so last-one-wins would apply a guarantee the
	/// caller did not intend.
	#[test]
	fn test_insert_with_rejects_a_repeated_option() {
		let error =
			parse_insert_error(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: a, not_before: b }"#);
		assert!(error.contains("not_before"), "the error must name the repeated option, got: {error}");
	}
}
