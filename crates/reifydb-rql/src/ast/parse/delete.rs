// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstDelete,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl Parser {
	pub(crate) fn parse_delete(&mut self) -> crate::Result<AstDelete> {
		let token = self.consume_keyword(Keyword::Delete)?;

		// Check if there's a target table specified (optional)
		let (schema, table) = if !self.is_eof()
			&& self.current()?.is_identifier()
		{
			let identifier = self.parse_identifier()?;

			if !self.is_eof()
				&& self.current_expect_operator(Operator::Dot)
					.is_ok()
			{
				self.consume_operator(Operator::Dot)?;
				let table = self.parse_identifier()?;
				(Some(identifier), Some(table))
			} else {
				(None, Some(identifier))
			}
		} else {
			// No target table specified - will be inferred from
			// input
			(None, None)
		};

		Ok(AstDelete {
			token,
			schema,
			table,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{AstDelete, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_schema_and_table() {
		let tokens = tokenize(
			r#"
        delete test.users
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		match delete {
			AstDelete {
				schema,
				table,
				..
			} => {
				assert_eq!(
					schema.as_ref().unwrap().value(),
					"test"
				);
				assert_eq!(
					table.as_ref().unwrap().value(),
					"users"
				);
			}
		}
	}

	#[test]
	fn test_table_only() {
		let tokens = tokenize(
			r#"
        delete users
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		match delete {
			AstDelete {
				schema,
				table,
				..
			} => {
				assert!(schema.is_none());
				assert_eq!(
					table.as_ref().unwrap().value(),
					"users"
				);
			}
		}
	}

	#[test]
	fn test_no_table() {
		let tokens = tokenize(
			r#"
        delete
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		match delete {
			AstDelete {
				schema,
				table,
				..
			} => {
				assert!(schema.is_none());
				assert!(table.is_none());
			}
		}
	}
}
