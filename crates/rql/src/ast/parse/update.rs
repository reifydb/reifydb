// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::{
	update_empty_assignments_block, update_missing_assignments_block, update_missing_filter_clause,
};
use reifydb_type::return_error;

use crate::ast::{
	ast::{Ast, AstUpdate},
	identifier::UnresolvedPrimitiveIdentifier,
	parse::Parser,
	tokenize::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl Parser {
	pub(crate) fn parse_update(&mut self) -> crate::Result<AstUpdate> {
		let token = self.consume_keyword(Keyword::Update)?;

		// 1. Parse target (REQUIRED) - namespace.table or just table
		if self.is_eof() || !matches!(self.current()?.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			return_error!(update_missing_assignments_block(token.fragment));
		}

		let first = self.parse_identifier_with_hyphens()?;
		let target = if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let second = self.parse_identifier_with_hyphens()?;
			UnresolvedPrimitiveIdentifier::new(Some(first.into_fragment()), second.into_fragment())
		} else {
			UnresolvedPrimitiveIdentifier::new(None, first.into_fragment())
		};

		// 2. Parse assignments block { name: 'value', ... } - REQUIRED
		if self.is_eof() || !self.current()?.is_operator(Operator::OpenCurly) {
			return_error!(update_missing_assignments_block(token.fragment));
		}
		let (assignments, _) = self.parse_expressions(true)?;
		if assignments.is_empty() {
			return_error!(update_empty_assignments_block(token.fragment));
		}

		// 3. Parse FILTER clause - REQUIRED
		if self.is_eof() || !self.current()?.is_keyword(Keyword::Filter) {
			return_error!(update_missing_filter_clause(token.fragment));
		}
		let filter = self.parse_filter()?;

		Ok(AstUpdate {
			token,
			target,
			assignments,
			filter: Box::new(Ast::Filter(filter)),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::ast::{
		ast::{Ast, InfixOperator},
		parse::Parser,
		tokenize::tokenize,
	};

	#[test]
	fn test_basic_update_syntax() {
		let tokens = tokenize(
			r#"
        UPDATE users { name: 'alice' } FILTER id == 1
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		// Check target
		assert!(update.target.namespace.is_none());
		assert_eq!(update.target.name.text(), "users");

		// Check assignments
		assert_eq!(update.assignments.len(), 1);
		let assignment = update.assignments[0].as_infix();
		assert!(matches!(assignment.operator, InfixOperator::As(_)));

		// Check filter exists
		assert!(matches!(*update.filter, Ast::Filter(_)));
	}

	#[test]
	fn test_update_with_namespace() {
		let tokens = tokenize(
			r#"
        UPDATE test.users { name: 'alice' } FILTER id == 1
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		// Check target with namespace
		assert_eq!(update.target.namespace.as_ref().unwrap().text(), "test");
		assert_eq!(update.target.name.text(), "users");
	}

	#[test]
	fn test_update_multiple_assignments() {
		let tokens = tokenize(
			r#"
        UPDATE users { name: 'alice', age: 30, active: true } FILTER id == 1
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		// Check we have 3 assignments
		assert_eq!(update.assignments.len(), 3);
	}

	#[test]
	fn test_update_complex_filter() {
		let tokens = tokenize(
			r#"
        UPDATE users { status: 'inactive' } FILTER age > 18 and active == true
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		// Check filter has AND operator
		let filter = update.filter.as_filter();
		let condition = filter.node.as_infix();
		assert!(matches!(condition.operator, InfixOperator::And(_)));
	}

	#[test]
	fn test_update_missing_filter_fails() {
		let tokens = tokenize(
			r#"
        UPDATE users { name: 'alice' }
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_update_missing_assignments_fails() {
		let tokens = tokenize(
			r#"
        UPDATE users FILTER id == 1
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_update_empty_assignments_fails() {
		let tokens = tokenize(
			r#"
        UPDATE users { } FILTER id == 1
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}
}
