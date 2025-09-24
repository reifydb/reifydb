// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstUpdate,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_update(&mut self) -> crate::Result<AstUpdate<'a>> {
		let token = self.consume_keyword(Keyword::Update)?;

		// Check if there's a target specified (optional)
		let target = if !self.is_eof() && self.current()?.is_identifier() {
			use crate::ast::identifier::UnresolvedSourceIdentifier;
			let first_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

			if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
				self.consume_operator(Operator::Dot)?;
				let second_token = self.advance()?;
				// namespace.source
				Some(UnresolvedSourceIdentifier::new(
					Some(first_token.fragment.clone()),
					second_token.fragment.clone(),
				))
			} else {
				// source only
				Some(UnresolvedSourceIdentifier::new(None, first_token.fragment.clone()))
			}
		} else {
			// No target specified - will be inferred from input
			None
		};

		Ok(AstUpdate {
			token,
			target,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{AstUpdate, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_namespace_and_table() {
		let tokens = tokenize(
			r#"
        update test.users
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		match update {
			AstUpdate {
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
        update users
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		match update {
			AstUpdate {
				target,
				..
			} => {
				let target = target.as_ref().expect("Should have target");
				assert!(target.namespace.is_none());
				assert_eq!(target.name.text(), "users");
			}
		}
	}

	#[test]
	fn test_no_table() {
		let tokens = tokenize(
			r#"
        update
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		match update {
			AstUpdate {
				target,
				..
			} => {
				assert!(target.is_none());
			}
		}
	}
}
