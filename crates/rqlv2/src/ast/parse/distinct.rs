// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DISTINCT expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Expr, expr::*},
	token::Punctuation,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse DISTINCT expression.
	pub(super) fn parse_distinct(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume DISTINCT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		// Parse columns (can be empty for DISTINCT {} meaning all columns)
		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let col = self.parse_expr(Precedence::Comparison)?;
			columns.push(*col);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Distinct(DistinctExpr::new(
			columns.into_bump_slice(),
			start_span.merge(&end_span),
		))))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Expr, token::tokenize};

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

	fn extract_distinct<'a>(
		stmt: crate::ast::Statement<'a>,
	) -> &'a crate::ast::expr::DistinctExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Distinct(d) => d,
			_ => panic!("Expected DISTINCT expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_distinct_empty() {
		let bump = Bump::new();
		let source = "DISTINCT {}";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let distinct = extract_distinct(stmt);

		assert_eq!(distinct.columns.len(), 0);
	}

	#[test]
	fn test_distinct_single_column() {
		let bump = Bump::new();
		let source = "DISTINCT { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let distinct = extract_distinct(stmt);

		assert_eq!(distinct.columns.len(), 1);
		match &distinct.columns[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_distinct_multiple_columns() {
		let bump = Bump::new();
		let source = "DISTINCT { name, age }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let distinct = extract_distinct(stmt);

		assert_eq!(distinct.columns.len(), 2);
		match &distinct.columns[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}
		match &distinct.columns[1] {
			Expr::Identifier(id) => assert_eq!(id.name, "age"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_distinct_lowercase() {
		let bump = Bump::new();
		let source = "distinct { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let distinct = extract_distinct(stmt);

		assert_eq!(distinct.columns.len(), 1);
	}
}
