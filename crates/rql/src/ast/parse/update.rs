// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{Ast, AstUpdate},
		identifier::UnresolvedPrimitiveIdentifier,
		parse::Parser,
	},
	bump::BumpBox,
	error::RqlError,
	token::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_update(&mut self) -> crate::Result<AstUpdate<'bump>> {
		let token = self.consume_keyword(Keyword::Update)?;

		// 1. Parse target (REQUIRED) - namespace.table or just table
		if self.is_eof() || !matches!(self.current()?.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			return Err(RqlError::UpdateMissingAssignmentsBlock {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let target = if segments.len() > 1 {
			let name = segments.pop().unwrap().into_fragment();
			let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
			UnresolvedPrimitiveIdentifier::new(namespace, name)
		} else {
			UnresolvedPrimitiveIdentifier::new(vec![], segments.remove(0).into_fragment())
		};

		// 2. Parse assignments block { name: 'value', ... } - REQUIRED
		if self.is_eof() || !self.current()?.is_operator(Operator::OpenCurly) {
			return Err(RqlError::UpdateMissingAssignmentsBlock {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		let (assignments, _) = self.parse_expressions(true, false)?;
		if assignments.is_empty() {
			return Err(RqlError::UpdateEmptyAssignmentsBlock {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		// 3. Parse FILTER clause - REQUIRED
		if self.is_eof() || !self.current()?.is_keyword(Keyword::Filter) {
			return Err(RqlError::UpdateMissingFilterClause {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		let filter = self.parse_filter()?;

		Ok(AstUpdate {
			token,
			target,
			assignments,
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
	fn test_basic_update_syntax() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        UPDATE users { name: 'alice' } FILTER {id == 1}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		assert!(update.target.namespace.is_empty());
		assert_eq!(update.target.name.text(), "users");

		assert_eq!(update.assignments.len(), 1);
		let assignment = update.assignments[0].as_infix();
		assert!(matches!(assignment.operator, InfixOperator::As(_)));

		assert!(matches!(*update.filter, Ast::Filter(_)));
	}

	#[test]
	fn test_update_with_namespace() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        UPDATE test::users { name: 'alice' } FILTER {id == 1}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		assert_eq!(update.target.namespace[0].text(), "test");
		assert_eq!(update.target.name.text(), "users");
	}

	#[test]
	fn test_update_multiple_assignments() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        UPDATE users { name: 'alice', age: 30, active: true } FILTER {id == 1}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		assert_eq!(update.assignments.len(), 3);
	}

	#[test]
	fn test_update_complex_filter() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        UPDATE users { status: 'inactive' } FILTER {age > 18 and active == true}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		let filter = update.filter.as_filter();
		let condition = filter.node.as_infix();
		assert!(matches!(condition.operator, InfixOperator::And(_)));
	}

	#[test]
	fn test_update_missing_filter_fails() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        UPDATE users { name: 'alice' }
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_update_missing_assignments_fails() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        UPDATE users FILTER id == 1
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_update_empty_assignments_fails() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        UPDATE users { } FILTER id == 1
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}
}
