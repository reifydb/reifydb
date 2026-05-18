// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstUpdate},
		identifier::UnresolvedShapeIdentifier,
		parse::Parser,
	},
	bump::BumpBox,
	error::{OperationKind, RqlError},
	token::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_update(&mut self) -> Result<AstUpdate<'bump>> {
		let token = self.consume_keyword(Keyword::Update)?;

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
			UnresolvedShapeIdentifier::new(namespace, name)
		} else {
			UnresolvedShapeIdentifier::new(vec![], segments.remove(0).into_fragment())
		};

		if self.is_eof() || !self.current()?.is_operator(Operator::OpenCurly) {
			return Err(RqlError::UpdateMissingAssignmentsBlock {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		let (assignments, _) = self.parse_expressions(true, false, None)?;
		if assignments.is_empty() {
			return Err(RqlError::UpdateEmptyAssignmentsBlock {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		if self.is_eof() || !self.current()?.is_keyword(Keyword::Filter) {
			return Err(RqlError::UpdateMissingFilterClause {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		let filter = self.parse_filter()?;

		let take = if !self.is_eof() && self.current()?.is_keyword(Keyword::Take) {
			let take = self.parse_take()?;
			Some(BumpBox::new_in(Ast::Take(take), self.bump()))
		} else {
			None
		};

		let returning = if !self.is_eof() && self.current()?.is_keyword(Keyword::Returning) {
			let returning_token = self.advance()?;
			let (exprs, had_braces) = self.parse_expressions(true, false, None)?;
			if !had_braces {
				return Err(RqlError::OperatorMissingBraces {
					kind: OperationKind::Returning,
					fragment: returning_token.fragment.to_owned(),
				}
				.into());
			}
			Some(exprs)
		} else {
			None
		};

		Ok(AstUpdate {
			token,
			target,
			assignments,
			filter: BumpBox::new_in(Ast::Filter(filter), self.bump()),
			take,
			returning,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstTakeValue, InfixOperator},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_basic_update_syntax() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users { name: 'alice' } FILTER {id == 1}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"
        UPDATE test::users { name: 'alice' } FILTER {id == 1}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = r#"
        UPDATE users { name: 'alice', age: 30, active: true } FILTER {id == 1}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		assert_eq!(update.assignments.len(), 3);
	}

	#[test]
	fn test_update_complex_filter() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users { status: 'inactive' } FILTER {age > 18 and active == true}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		let filter = update.filter.as_filter();
		let condition = filter.node.as_infix();
		assert!(matches!(condition.operator, InfixOperator::And(_)));
	}

	#[test]
	fn test_update_with_take() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users { x: 1 } FILTER {id > 0} TAKE 10
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		let take = update.take.as_ref().unwrap().as_take();
		assert_eq!(take.take, AstTakeValue::Literal(10));
	}

	#[test]
	fn test_update_with_take_variable() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users { x: 1 } FILTER {id > 0} TAKE $limit
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		let take = update.take.as_ref().unwrap().as_take();
		assert!(matches!(take.take, AstTakeValue::Variable(_)));
	}

	#[test]
	fn test_update_without_take() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users { x: 1 } FILTER {id > 0}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let update = result.first_unchecked().as_update();

		assert!(update.take.is_none());
	}

	#[test]
	fn test_update_missing_filter_fails() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users { name: 'alice' }
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_update_missing_assignments_fails() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users FILTER id == 1
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_update_empty_assignments_fails() {
		let bump = Bump::new();
		let source = r#"
        UPDATE users { } FILTER id == 1
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}
}
