// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstDelete,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_delete(&mut self) -> crate::Result<AstDelete<'a>> {
		let token = self.consume_keyword(Keyword::Delete)?;

		// Check if there's a target table specified (optional)
		let target = if !self.is_eof() && self.current()?.is_identifier() {
			use crate::ast::identifier::MaybeQualifiedTableIdentifier;
			let first_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

			if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
				self.consume_operator(Operator::Dot)?;
				let second_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;
				// namespace.table
				Some(MaybeQualifiedTableIdentifier::new(second_token.fragment.clone())
					.with_namespace(first_token.fragment.clone()))
			} else {
				// table only
				Some(MaybeQualifiedTableIdentifier::new(first_token.fragment.clone()))
			}
		} else {
			// No target table specified - will be inferred from
			// input
			None
		};

		Ok(AstDelete {
			token,
			target,
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
				target,
				..
			} => {
				let target = target.as_ref().unwrap();
				assert_eq!(target.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(target.name.text(), "users");
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
				target,
				..
			} => {
				let target = target.as_ref().unwrap();
				assert!(target.namespace.is_none());
				assert_eq!(target.name.text(), "users");
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
				target,
				..
			} => {
				assert!(target.is_none());
			}
		}
	}
}
