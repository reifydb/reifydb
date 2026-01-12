// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! AGGREGATE expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Expr, expr::*},
	token::{Keyword, Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse AGGREGATE expression: AGGREGATE { expr, ... } BY { col, ... }
	pub(super) fn parse_aggregate(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume AGGREGATE

		// Require opening brace for aggregations
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut aggregations = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let agg = self.parse_expr(Precedence::None)?;
			aggregations.push(*agg);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		// Require BY keyword
		self.expect_keyword(Keyword::By)?;

		// Require opening brace for group-by columns
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut group_by = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let col = self.parse_expr(Precedence::Comparison)?;
			group_by.push(*col);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Aggregate(AggregateExpr::new(
			group_by.into_bump_slice(),
			aggregations.into_bump_slice(),
			start_span.merge(&end_span),
		))))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Expr, ast::expr::BinaryOp, token::tokenize};

	fn get_first_expr<'a>(stmt: crate::ast::Statement<'a>) -> &'a Expr<'a> {
		match stmt {
			crate::ast::Statement::Pipeline(p) => {
				assert!(!p.stages.is_empty());
				&p.stages[0]
			}
			crate::ast::Statement::Expression(e) => e.expr,
			_ => panic!("Expected Pipeline or Expression statement"),
		}
	}

	fn extract_aggregate<'a>(
		stmt: crate::ast::Statement<'a>,
	) -> &'a crate::ast::expr::AggregateExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Aggregate(a) => a,
			_ => panic!("Expected AGGREGATE expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_aggregate_single_column() {
		let bump = Bump::new();
		let source = "AGGREGATE { min(age) } BY { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 1);
		assert_eq!(agg.group_by.len(), 1);

		// Check group_by
		match &agg.group_by[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier in group_by"),
		}

		// Check aggregation is a function call
		match &agg.aggregations[0] {
			Expr::Call(call) => {
				match call.function {
					Expr::Identifier(id) => assert_eq!(id.name, "min"),
					_ => panic!("Expected identifier for function name"),
				}
				assert_eq!(call.arguments.len(), 1);
			}
			_ => panic!("Expected function call in aggregations"),
		}
	}

	#[test]
	fn test_aggregate_keyword_as_identifier() {
		let bump = Bump::new();
		let source = "AGGREGATE { min(value) } BY { value }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 1);
		assert_eq!(agg.group_by.len(), 1);

		match &agg.group_by[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "value"),
			_ => panic!("Expected identifier in group_by"),
		}
	}

	#[test]
	fn test_aggregate_with_alias() {
		let bump = Bump::new();
		let source = "AGGREGATE { min(age) as min_age } BY { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 1);

		// Should be an AS expression
		match &agg.aggregations[0] {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::As);
				match binary.right {
					Expr::Identifier(id) => assert_eq!(id.name, "min_age"),
					_ => panic!("Expected identifier alias"),
				}
			}
			_ => panic!("Expected binary AS expression"),
		}
	}

	#[test]
	fn test_aggregate_colon_alias() {
		let bump = Bump::new();
		let source = "AGGREGATE { min_age: min(age) } BY { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 1);
		// Should be a KeyValue expression (colon syntax alias)
		match &agg.aggregations[0] {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::KeyValue);
				match binary.left {
					Expr::Identifier(id) => assert_eq!(id.name, "min_age"),
					_ => panic!("Expected identifier alias"),
				}
				match binary.right {
					Expr::Call(call) => {
						match call.function {
							Expr::Identifier(id) => assert_eq!(id.name, "min"),
							_ => panic!("Expected function identifier"),
						}
					}
					_ => panic!("Expected function call"),
				}
			}
			_ => panic!("Expected binary KeyValue expression"),
		}
	}

	#[test]
	fn test_aggregate_multiple_columns() {
		let bump = Bump::new();
		let source = "AGGREGATE { min(age), max(age) } BY { name, gender }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 2);
		assert_eq!(agg.group_by.len(), 2);

		// Check first group_by
		match &agg.group_by[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}

		// Check second group_by
		match &agg.group_by[1] {
			Expr::Identifier(id) => assert_eq!(id.name, "gender"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_aggregate_empty_by_clause() {
		// Global aggregation
		let bump = Bump::new();
		let source = "AGGREGATE { count(value) } BY {}";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 1);
		assert_eq!(agg.group_by.len(), 0);
		// Verify COUNT function
		match &agg.aggregations[0] {
			Expr::Call(call) => {
				match call.function {
					Expr::Identifier(id) => assert_eq!(id.name, "count"),
					_ => panic!("Expected identifier for function name"),
				}
				assert_eq!(call.arguments.len(), 1);
			}
			_ => panic!("Expected function call"),
		}
	}

	#[test]
	fn test_aggregate_lowercase() {
		let bump = Bump::new();
		let source = "aggregate { min(age) } by { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 1);
		assert_eq!(agg.group_by.len(), 1);
		// Verify min function
		match &agg.aggregations[0] {
			Expr::Call(call) => {
				match call.function {
					Expr::Identifier(id) => assert_eq!(id.name, "min"),
					_ => panic!("Expected identifier for function name"),
				}
			}
			_ => panic!("Expected function call"),
		}
		// Verify group_by
		match &agg.group_by[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier in group_by"),
		}
	}

	#[test]
	fn test_aggregate_single_aggregation() {
		let bump = Bump::new();
		let source = "AGGREGATE { min(age) } BY { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.aggregations.len(), 1);

		match &agg.aggregations[0] {
			Expr::Call(call) => {
				match call.function {
					Expr::Identifier(id) => assert_eq!(id.name, "min"),
					_ => panic!("Expected identifier for function name"),
				}
			}
			_ => panic!("Expected function call"),
		}
	}

	#[test]
	fn test_aggregate_single_by() {
		let bump = Bump::new();
		let source = "AGGREGATE { count(id) } BY { department }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let agg = extract_aggregate(stmt);

		assert_eq!(agg.group_by.len(), 1);
		match &agg.group_by[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "department"),
			_ => panic!("Expected identifier"),
		}
	}
}
