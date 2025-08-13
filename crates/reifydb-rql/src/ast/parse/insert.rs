// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstInsert,
	lex::{Keyword, Operator},
	parse::Parser,
};

impl Parser {
	pub(crate) fn parse_insert(&mut self) -> crate::Result<AstInsert> {
		let token = self.consume_keyword(Keyword::Insert)?;

		let identifier = self.parse_identifier()?;

		let (schema, table) = if self
			.current_expect_operator(Operator::Dot)
			.is_ok()
		{
			self.consume_operator(Operator::Dot)?;
			let table = self.parse_identifier()?;
			(Some(identifier), table)
		} else {
			(None, identifier)
		};

		Ok(AstInsert {
			token,
			schema,
			table,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{AstInsert, lex::lex, parse::Parser};

	#[test]
	fn test_schema_and_table() {
		let tokens = lex(r#"
        insert test.users
    "#)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		match insert {
			AstInsert {
				schema,
				table,
				..
			} => {
				assert_eq!(
					schema.as_ref().unwrap().value(),
					"test"
				);
				assert_eq!(table.value(), "users");
			}
		}
	}

	#[test]
	fn test_table_only() {
		let tokens = lex(r#"
        insert users
    "#)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		match insert {
			AstInsert {
				schema,
				table,
				..
			} => {
				assert!(schema.is_none());
				assert_eq!(table.value(), "users");
			}
		}
	}
}
