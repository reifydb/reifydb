// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	Ast, AstList, TokenKind,
	ast::AstFrom,
	lex::{
		Keyword, Operator,
		Operator::{CloseBracket, OpenBracket},
		Separator,
	},
	parse::Parser,
};

impl Parser {
	pub(crate) fn parse_from(&mut self) -> crate::Result<AstFrom> {
		let token = self.consume_keyword(Keyword::From)?;

		if self.current()?.is_operator(OpenBracket) {
			Ok(AstFrom::Static {
				token,
				list: self.parse_static()?,
			})
		} else {
			let identifier = self.parse_as_identifier()?;

			let (schema, table) = if !self.is_eof()
				&& self.current()?.is_operator(Operator::Dot)
			{
				self.consume_operator(Operator::Dot)?;
				let table = self.parse_as_identifier()?;
				(Some(identifier), table)
			} else {
				(None, identifier)
			};

			Ok(AstFrom::Table {
				token,
				schema,
				table,
			})
		}
	}

	pub(crate) fn parse_static(&mut self) -> crate::Result<AstList> {
		let token = self.consume_operator(OpenBracket)?;

		let mut nodes = Vec::new();
		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(CloseBracket) {
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
	use crate::ast::{AstFrom, lex::lex, parse::Parser};

	#[test]
	fn test_from_schema_and_table() {
		let tokens = lex("FROM reifydb.users").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Table {
				table,
				schema,
				..
			} => {
				assert_eq!(
					schema.as_ref().unwrap().value(),
					"reifydb"
				);
				assert_eq!(table.value(), "users");
			}
			AstFrom::Static {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_table_without_schema() {
		let tokens = lex("FROM users").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Table {
				table,
				schema,
				..
			} => {
				assert_eq!(schema, &None);
				assert_eq!(table.value(), "users");
			}
			AstFrom::Static {
				..
			} => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_empty() {
		let tokens = lex("FROM []").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Table {
				..
			} => unreachable!(),
			AstFrom::Static {
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
		let tokens = lex("FROM [ { field: 'value' }]").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Table {
				..
			} => unreachable!(),
			AstFrom::Static {
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
		let tokens = lex("FROM [ { field: 'value' },\
        { field: 'value2' }\
        ]")
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Table {
				..
			} => unreachable!(),
			AstFrom::Static {
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
	fn test_from_static_trailing_comma() {
		let tokens = lex("FROM [ { field: 'value' }, ]").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Table {
				..
			} => unreachable!(),
			AstFrom::Static {
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
