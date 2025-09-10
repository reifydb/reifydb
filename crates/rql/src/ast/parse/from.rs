// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	Ast, AstList, TokenKind,
	ast::AstFrom,
	parse::Parser,
	tokenize::{
		Keyword, Operator,
		Operator::{CloseBracket, OpenBracket},
		Separator,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_from(&mut self) -> crate::Result<AstFrom<'a>> {
		let token = self.consume_keyword(Keyword::From)?;

		// Check token type first
		let is_inline = if let Ok(current) = self.current() {
			current.is_operator(OpenBracket)
		} else {
			false
		};

		if is_inline {
			Ok(AstFrom::Inline {
				token,
				list: self.parse_static()?,
			})
		} else {
			use reifydb_core::interface::identifier::SourceKind;

			use crate::ast::identifier::MaybeQualifiedSourceIdentifier;

			// Get the first identifier token
			let first_token =
				self.consume(TokenKind::Identifier)?;

			// Check if there's a dot following
			let has_dot = if !self.is_eof() {
				if let Ok(current) = self.current() {
					current.is_operator(Operator::Dot)
				} else {
					false
				}
			} else {
				false
			};

			let (source, alias_token) = if has_dot {
				self.consume_operator(Operator::Dot)?;
				let second_token =
					self.consume(TokenKind::Identifier)?;
				// schema.table - create
				// MaybeQualifiedSourceIdentifier with schema
				let source =
					MaybeQualifiedSourceIdentifier::new(
						second_token.fragment.clone(),
					)
					.with_schema(
						first_token.fragment.clone(),
					)
					.with_kind(SourceKind::Unknown); // Will be resolved later

				// Check for alias after schema.table
				let alias_token = if !self.is_eof()
					&& self.current()?.is_identifier()
				{
					Some(self.consume(
						TokenKind::Identifier,
					)?)
				} else {
					None
				};

				(source, alias_token)
			} else {
				// Just table - create
				// MaybeQualifiedSourceIdentifier without schema
				let source =
					MaybeQualifiedSourceIdentifier::new(
						first_token.fragment.clone(),
					)
					.with_kind(SourceKind::Unknown); // Will be resolved later

				// Check for alias after table
				let alias_token = if !self.is_eof()
					&& self.current()?.is_identifier()
				{
					Some(self.consume(
						TokenKind::Identifier,
					)?)
				} else {
					None
				};

				(source, alias_token)
			};

			// Add alias to source if present
			let source = if let Some(alias_tok) = alias_token {
				source.with_alias(alias_tok.fragment.clone())
			} else {
				source
			};

			// Check for index directive using ::
			let index_name =
				if !self.is_eof() {
					if let Ok(current) = self.current() {
						if current.is_operator(
							Operator::DoubleColon,
						) {
							self.consume_operator(Operator::DoubleColon)?;
							let index_token = self.consume(TokenKind::Identifier)?;
							Some(crate::ast::ast::AstIdentifier(index_token))
						} else {
							None
						}
					} else {
						None
					}
				} else {
					None
				};

			Ok(AstFrom::Source {
				token,
				source,
				index_name,
			})
		}
	}

	pub(crate) fn parse_static(&mut self) -> crate::Result<AstList<'a>> {
		let token = self.consume_operator(OpenBracket)?;

		let mut nodes = Vec::new();
		loop {
			self.skip_new_line()?;

			// Check if we've reached the closing bracket
			let should_break = if let Ok(current) = self.current() {
				current.is_operator(CloseBracket)
			} else {
				true
			};

			if should_break {
				break;
			}

			nodes.push(Ast::Inline(self.parse_inline()?));

			self.consume_if(TokenKind::Separator(
				Separator::Comma,
			))?;
		}

		self.consume_operator(CloseBracket)?;
		Ok(AstList {
			token,
			nodes,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{AstFrom, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_from_schema_and_table() {
		let tokens = tokenize("FROM reifydb.users").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(
					source.schema.as_ref().unwrap().text(),
					"reifydb"
				);
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name, &None);
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_table_without_schema() {
		let tokens = tokenize("FROM users").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(source.schema, None);
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name, &None);
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_empty() {
		let tokens = tokenize("FROM []").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				..
			} => unreachable!(),
			AstFrom::Inline {
				list: query,
				..
			} => {
				let block = query;
				assert_eq!(block.len(), 0);
			}
		}
	}

	#[test]
	fn test_from_static() {
		let tokens = tokenize("FROM [ { field: 'value' }]").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				..
			} => unreachable!(),
			AstFrom::Inline {
				list,
				..
			} => {
				assert_eq!(list.len(), 1);

				let row = list[0].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(
					row.keyed_values[0].key.value(),
					"field"
				);
				assert_eq!(
					row.keyed_values[0]
						.value
						.as_literal_text()
						.value(),
					"value"
				);
			}
		}
	}

	#[test]
	fn test_from_static_multiple() {
		let tokens = tokenize(
			"FROM [ { field: 'value' },\
        { field: 'value2' }\
        ]",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				..
			} => unreachable!(),
			AstFrom::Inline {
				list,
				..
			} => {
				assert_eq!(list.len(), 2);

				let row = list[0].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(
					row.keyed_values[0].key.value(),
					"field"
				);
				assert_eq!(
					row.keyed_values[0]
						.value
						.as_literal_text()
						.value(),
					"value"
				);

				let row = list[1].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(
					row.keyed_values[0].key.value(),
					"field"
				);
				assert_eq!(
					row.keyed_values[0]
						.value
						.as_literal_text()
						.value(),
					"value2"
				);
			}
		}
	}

	#[test]
	fn test_from_with_index_directive() {
		let tokens = tokenize("FROM users::user_id_pk").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(source.schema, None);
				assert_eq!(source.name.text(), "users");
				assert_eq!(
					index_name.as_ref().unwrap().value(),
					"user_id_pk"
				);
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_schema_table_with_index_directive() {
		let tokens =
			tokenize("FROM company.employees::employee_email_pk")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(
					source.schema.as_ref().unwrap().text(),
					"company"
				);
				assert_eq!(source.name.text(), "employees");
				assert_eq!(
					index_name.as_ref().unwrap().value(),
					"employee_email_pk"
				);
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_table_with_alias() {
		let tokens = tokenize("FROM orders o").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert!(source.schema.is_none());
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(
					source.alias.as_ref().unwrap().text(),
					"o"
				);
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_schema_table_with_alias() {
		let tokens = tokenize("FROM test.orders o").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(
					source.schema.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(
					source.alias.as_ref().unwrap().text(),
					"o"
				);
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_trailing_comma() {
		let tokens = tokenize("FROM [ { field: 'value' }, ]").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				..
			} => unreachable!(),
			AstFrom::Inline {
				list,
				..
			} => {
				assert_eq!(list.len(), 1);

				let row = list[0].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(
					row.keyed_values[0].key.value(),
					"field"
				);
				assert_eq!(
					row.keyed_values[0]
						.value
						.as_literal_text()
						.value(),
					"value"
				);
			}
		}
	}
}
