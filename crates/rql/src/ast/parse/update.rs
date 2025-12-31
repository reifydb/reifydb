// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::ast::{
	AstUpdate,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl Parser {
	pub(crate) fn parse_update(&mut self) -> crate::Result<AstUpdate> {
		let token = self.consume_keyword(Keyword::Update)?;

		// Check if there's a target specified (optional)
		let target = if !self.is_eof()
			&& matches!(
				self.current()?.kind,
				crate::ast::tokenize::TokenKind::Identifier
					| crate::ast::tokenize::TokenKind::Keyword(_)
			) {
			use crate::ast::identifier::UnresolvedPrimitiveIdentifier;
			let first = self.parse_identifier_with_hyphens()?;

			if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
				self.consume_operator(Operator::Dot)?;
				let second = self.parse_identifier_with_hyphens()?;
				// namespace.source
				Some(UnresolvedPrimitiveIdentifier::new(
					Some(first.into_fragment()),
					second.into_fragment(),
				))
			} else {
				// source only
				Some(UnresolvedPrimitiveIdentifier::new(None, first.into_fragment()))
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
