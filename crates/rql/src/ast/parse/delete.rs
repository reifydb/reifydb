// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstDelete},
		identifier::UnresolvedSchemaIdentifier,
		parse::Parser,
	},
	bump::BumpBox,
	error::{OperationKind, RqlError},
	token::{keyword::Keyword, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_delete(&mut self) -> Result<AstDelete<'bump>> {
		let token = self.consume_keyword(Keyword::Delete)?;

		// 1. Parse target (REQUIRED) - namespace.table or just table
		if self.is_eof() || !matches!(self.current()?.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			return Err(RqlError::DeleteMissingTarget {
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let target = if segments.len() > 1 {
			let name = segments.pop().unwrap().into_fragment();
			let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
			UnresolvedSchemaIdentifier::new(namespace, name)
		} else {
			UnresolvedSchemaIdentifier::new(vec![], segments.remove(0).into_fragment())
		};

		// 2. Parse FILTER clause - REQUIRED
		if self.is_eof() || !self.current()?.is_keyword(Keyword::Filter) {
			return Err(RqlError::DeleteMissingFilterClause {
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

		Ok(AstDelete {
			token,
			target,
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
	fn test_basic_delete_syntax() {
		let bump = Bump::new();
		let source = r#"
        DELETE users FILTER {id == 1}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		assert!(delete.target.namespace.is_empty());
		assert_eq!(delete.target.name.text(), "users");

		assert!(matches!(*delete.filter, Ast::Filter(_)));
	}

	#[test]
	fn test_delete_with_namespace() {
		let bump = Bump::new();
		let source = r#"
        DELETE test::users FILTER {id == 1}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		assert_eq!(delete.target.namespace[0].text(), "test");
		assert_eq!(delete.target.name.text(), "users");
	}

	#[test]
	fn test_delete_complex_filter() {
		let bump = Bump::new();
		let source = r#"
        DELETE users FILTER {age > 18 and active == false}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		let filter = delete.filter.as_filter();
		let condition = filter.node.as_infix();
		assert!(matches!(condition.operator, InfixOperator::And(_)));
	}

	#[test]
	fn test_delete_with_take() {
		let bump = Bump::new();
		let source = r#"
        DELETE users FILTER {id > 0} TAKE 5
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		let take = delete.take.as_ref().unwrap().as_take();
		assert_eq!(take.take, AstTakeValue::Literal(5));
	}

	#[test]
	fn test_delete_with_take_and_returning() {
		let bump = Bump::new();
		let source = r#"
        DELETE users FILTER {id > 0} TAKE 5 RETURNING { id }
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		let take = delete.take.as_ref().unwrap().as_take();
		assert_eq!(take.take, AstTakeValue::Literal(5));
		assert!(delete.returning.is_some());
	}

	#[test]
	fn test_delete_without_take() {
		let bump = Bump::new();
		let source = r#"
        DELETE users FILTER {id > 0}
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let delete = result.first_unchecked().as_delete();

		assert!(delete.take.is_none());
	}

	#[test]
	fn test_delete_missing_filter_fails() {
		let bump = Bump::new();
		let source = r#"
        DELETE users
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}

	#[test]
	fn test_delete_missing_target_fails() {
		let bump = Bump::new();
		let source = r#"
        DELETE FILTER id == 1
    "#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err());
	}
}
