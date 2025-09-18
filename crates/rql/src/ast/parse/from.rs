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
			use reifydb_core::interface::identifier::UnresolvedSourceIdentifier;

			// Get the first identifier token
			let first_token = self.consume(TokenKind::Identifier)?;

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

			let source = if has_dot {
				self.consume_operator(Operator::Dot)?;
				let second_token = self.advance()?;

				// namespace.table - create
				// UnresolvedSourceIdentifier with namespace
				let mut source = UnresolvedSourceIdentifier::new(
					Some(first_token.fragment.clone()),
					second_token.fragment.clone(),
				);

				// Check for alias after namespace.table
				if !self.is_eof() && self.current()?.is_identifier() {
					let alias_token = self.consume(TokenKind::Identifier)?;
					source = source.with_alias(alias_token.fragment.clone());
				}

				source
			} else {
				// Just table - create
				// UnresolvedSourceIdentifier without namespace
				let mut source = UnresolvedSourceIdentifier::new(None, first_token.fragment.clone());

				// Check for alias after table
				if !self.is_eof() && self.current()?.is_identifier() {
					let alias_token = self.consume(TokenKind::Identifier)?;
					source = source.with_alias(alias_token.fragment.clone());
				}

				source
			};

			// Check for index directive using ::
			let index_name = if !self.is_eof() {
				if let Ok(current) = self.current() {
					if current.is_operator(Operator::DoubleColon) {
						self.consume_operator(Operator::DoubleColon)?;
						let index_token = self.consume(TokenKind::Identifier)?;
						Some(index_token.fragment)
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

			self.consume_if(TokenKind::Separator(Separator::Comma))?;
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
				assert_eq!(source.namespace.as_ref().unwrap().text(), "reifydb");
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
				assert_eq!(source.namespace, None);
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

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value");
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

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value");

				let row = list[1].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value2");
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
				assert_eq!(source.namespace, None);
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name.as_ref().unwrap().text(), "user_id_pk");
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_namespace_table_with_index_directive() {
		let tokens = tokenize("FROM company.employees::employee_email_pk").unwrap();
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
				assert_eq!(source.namespace.as_ref().unwrap().text(), "company");
				assert_eq!(source.name.text(), "employees");
				assert_eq!(index_name.as_ref().unwrap().text(), "employee_email_pk");
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
				assert!(source.namespace.is_none());
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(source.alias.as_ref().unwrap().text(), "o");
			}
			AstFrom::Inline {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_namespace_table_with_alias() {
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
				assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(source.alias.as_ref().unwrap().text(), "o");
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

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value");
			}
		}
	}
}
