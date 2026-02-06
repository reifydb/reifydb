// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::{delete_missing_filter_clause, delete_missing_target};
use reifydb_type::return_error;

use crate::{
	ast::{
		ast::{Ast, AstDelete},
		identifier::UnresolvedPrimitiveIdentifier,
		parse::Parser,
	},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_delete(&mut self) -> crate::Result<AstDelete<'bump>> {
		let token = self.consume_keyword(Keyword::Delete)?;

		// 1. Parse target (REQUIRED) - namespace.table or just table
		if self.is_eof() || !matches!(self.current()?.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			return_error!(delete_missing_target(token.fragment.to_owned()));
		}

		let first = self.parse_identifier_with_hyphens()?;
		let target = if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let second = self.parse_identifier_with_hyphens()?;
			UnresolvedPrimitiveIdentifier::new(Some(first.into_fragment()), second.into_fragment())
		} else {
			UnresolvedPrimitiveIdentifier::new(None, first.into_fragment())
		};

		// 2. Parse FILTER clause - REQUIRED
		if self.is_eof() || !self.current()?.is_keyword(Keyword::Filter) {
			return_error!(delete_missing_filter_clause(token.fragment.to_owned()));
		}
		let filter = self.parse_filter()?;

		Ok(AstDelete {
			token,
			target,
			filter: BumpBox::new_in(Ast::Filter(filter), self.bump()),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, InfixOperator},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_basic_delete_syntax() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        DELETE users FILTER {id == 1}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		assert!(delete.target.namespace.is_none());
		assert_eq!(delete.target.name.text(), "users");

		assert!(matches!(*delete.filter, Ast::Filter(_)));
	}

	#[test]
	fn test_delete_with_namespace() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        DELETE test.users FILTER {id == 1}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		assert_eq!(delete.target.namespace.as_ref().unwrap().text(), "test");
		assert_eq!(delete.target.name.text(), "users");
	}

	#[test]
	fn test_delete_complex_filter() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        DELETE users FILTER {age > 18 and active == false}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		let filter = delete.filter.as_filter();
		let condition = filter.node.as_infix();
		assert!(matches!(condition.operator, InfixOperator::And(_)));
	}

	#[test]
	fn test_delete_missing_filter_fails() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        DELETE users
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_delete_missing_target_fails() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        DELETE FILTER id == 1
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}
}
