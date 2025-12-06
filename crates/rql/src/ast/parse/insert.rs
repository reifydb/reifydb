// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstInsert,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_insert(&mut self) -> crate::Result<AstInsert<'a>> {
		let token = self.consume_keyword(Keyword::Insert)?;

		use crate::ast::identifier::UnresolvedSourceIdentifier;
		let first = self.parse_identifier_with_hyphens()?;

		let target = if self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let second = self.parse_identifier_with_hyphens()?;
			// namespace.source
			Some(UnresolvedSourceIdentifier::new(Some(first.into_fragment()), second.into_fragment()))
		} else {
			// source only
			Some(UnresolvedSourceIdentifier::new(None, first.into_fragment()))
		};

		Ok(AstInsert {
			token,
			target,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{AstInsert, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_schema_and_table() {
		let tokens = tokenize(
			r#"
        insert test.users
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		match insert {
			AstInsert {
				target,
				..
			} => {
				let target = target.as_ref().expect("Should have target");
				assert_eq!(target.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(target.name.text(), "users");
			}
		}
	}

	#[test]
	fn test_table_only() {
		let tokens = tokenize(
			r#"
        insert users
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let insert = result.first_unchecked().as_insert();

		match insert {
			AstInsert {
				target,
				..
			} => {
				let target = target.as_ref().expect("Should have target");
				assert!(target.namespace.is_none());
				assert_eq!(target.name.text(), "users");
			}
		}
	}
}
