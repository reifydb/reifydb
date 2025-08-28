// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstAlter, AstAlterSequence,
	parse::Parser,
	tokenize::{Keyword, Operator, Token},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_alter(&mut self) -> crate::Result<AstAlter<'a>> {
		let token = self.consume_keyword(Keyword::Alter)?;

		if self.current()?.is_keyword(Keyword::Sequence) {
			self.consume_keyword(Keyword::Sequence)?;
			return self.parse_alter_sequence(token);
		}

		unimplemented!("Only ALTER SEQUENCE is supported");
	}

	fn parse_alter_sequence(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstAlter<'a>> {
		// Parse schema.table.column or table.column
		let first_identifier_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;

		if self.current()?.is_operator(Operator::Dot) {
			self.consume_operator(Operator::Dot)?;
			let second_identifier_token = self.consume(
				crate::ast::tokenize::TokenKind::Identifier,
			)?;

			if self.current()?.is_operator(Operator::Dot) {
				self.consume_operator(Operator::Dot)?;
				let column_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

				// Expect SET VALUE <number>
				self.consume_keyword(Keyword::Set)?;
				self.consume_keyword(Keyword::Value)?;
				let value_token = self.consume(crate::ast::tokenize::TokenKind::Literal(crate::ast::tokenize::Literal::Number))?;

				// Create AST nodes from tokens
				let first_identifier =
					crate::ast::ast::AstIdentifier(
						first_identifier_token,
					);
				let second_identifier =
					crate::ast::ast::AstIdentifier(
						second_identifier_token,
					);
				let column = crate::ast::ast::AstIdentifier(
					column_token,
				);
				let value = crate::ast::AstLiteral::Number(
					crate::ast::ast::AstLiteralNumber(
						value_token,
					),
				);

				Ok(AstAlter::Sequence(AstAlterSequence {
					token,
					schema: Some(first_identifier),
					table: second_identifier,
					column,
					value,
				}))
			} else {
				// table.column
				self.consume_keyword(Keyword::Set)?;
				self.consume_keyword(Keyword::Value)?;
				let value_token = self.consume(crate::ast::tokenize::TokenKind::Literal(crate::ast::tokenize::Literal::Number))?;

				// Create AST nodes from tokens
				let first_identifier =
					crate::ast::ast::AstIdentifier(
						first_identifier_token,
					);
				let second_identifier =
					crate::ast::ast::AstIdentifier(
						second_identifier_token,
					);
				let value = crate::ast::AstLiteral::Number(
					crate::ast::ast::AstLiteralNumber(
						value_token,
					),
				);

				Ok(AstAlter::Sequence(AstAlterSequence {
					token,
					schema: None,
					table: first_identifier,
					column: second_identifier,
					value,
				}))
			}
		} else {
			unimplemented!(
				"ALTER SEQUENCE requires table.column or schema.table.column"
			);
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{
		AstAlter, AstAlterSequence, parse::Parser, tokenize::tokenize,
	};

	#[test]
	fn test_alter_sequence_with_schema() {
		let tokens =
			tokenize("ALTER SEQUENCE test.users.id SET VALUE 1000")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Sequence(AstAlterSequence {
				schema,
				table,
				column,
				value,
				..
			}) => {
				assert!(schema.is_some());
				assert_eq!(
					schema.as_ref().unwrap().value(),
					"test"
				);
				assert_eq!(table.value(), "users");
				assert_eq!(column.value(), "id");
				match value {
					crate::ast::AstLiteral::Number(num) => {
						assert_eq!(num.value(), "1000")
					}
					_ => panic!("Expected number literal"),
				}
			}
		}
	}

	#[test]
	fn test_alter_sequence_without_schema() {
		let tokens = tokenize("ALTER SEQUENCE users.id SET VALUE 500")
			.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Sequence(AstAlterSequence {
				schema,
				table,
				column,
				value,
				..
			}) => {
				assert!(schema.is_none());
				assert_eq!(table.value(), "users");
				assert_eq!(column.value(), "id");
				match value {
					crate::ast::AstLiteral::Number(num) => {
						assert_eq!(num.value(), "500")
					}
					_ => panic!("Expected number literal"),
				}
			}
		}
	}

	#[test]
	fn test_alter_sequence_case_insensitive() {
		let tokens =
			tokenize("alter sequence TEST.USERS.ID set value 2000")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Sequence(AstAlterSequence {
				schema,
				table,
				column,
				value,
				..
			}) => {
				assert!(schema.is_some());
				assert_eq!(
					schema.as_ref().unwrap().value(),
					"TEST"
				);
				assert_eq!(table.value(), "USERS");
				assert_eq!(column.value(), "ID");
				match value {
					crate::ast::AstLiteral::Number(num) => {
						assert_eq!(num.value(), "2000")
					}
					_ => panic!("Expected number literal"),
				}
			}
		}
	}
}
