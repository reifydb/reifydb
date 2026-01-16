// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! APPLY expression parsing.
//!
//! RQL syntax:
//! - `APPLY operator {}` - empty block
//! - `APPLY operator { expr1, expr2, ... }` - multiple expressions

use bumpalo::collections::Vec as BumpVec;

use super::{ParseError, ParseErrorKind, Parser, Precedence};
use crate::{
	ast::{Expr, expr::special::ApplyExpr},
	token::{keyword::Keyword, punctuation::Punctuation, token::TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse APPLY expression.
	///
	/// Syntax:
	/// - `APPLY operator {}` - empty block
	/// - `APPLY operator { expr1, expr2, ... }` - multiple expressions
	///
	/// # Examples
	///
	/// ```rql
	/// APPLY counter {}
	/// APPLY process { a, b, c }
	/// ```
	pub(in crate::ast::parse) fn parse_apply(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Apply)?;

		// Parse operator name (identifier or keyword)
		let operator = match &self.current().kind {
			TokenKind::Identifier => {
				let token = self.current();
				let text = self.token_text(token);
				self.advance();
				text
			}
			TokenKind::Keyword(_) => {
				let token = self.current();
				let text = self.token_text(token);
				self.advance();
				text
			}
			_ => return Err(self.error(ParseErrorKind::ExpectedIdentifier)),
		};

		// Always require braces: { expr, expr, ... } or {}
		self.expect_punct(Punctuation::OpenCurly)?;
		let mut expressions = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let expr = self.parse_expr(Precedence::None)?;
			expressions.push(*expr);

			self.skip_newlines();

			// Check for comma
			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end = self.expect_punct(Punctuation::CloseCurly)?;
		let span = start.merge(&end);

		Ok(self.alloc(Expr::Apply(ApplyExpr {
			operator,
			expressions: expressions.into_bump_slice(),
			span,
		})))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{Expr, expr::special::ApplyExpr, parse::parse},
		token::tokenize,
	};

	fn get_first_expr(stmt: crate::ast::Statement<'_>) -> &Expr<'_> {
		match stmt {
			crate::ast::Statement::Pipeline(p) => {
				assert!(!p.stages.is_empty());
				&p.stages[0]
			}
			crate::ast::Statement::Expression(e) => e.expr,
			_ => panic!("Expected Pipeline or Expression statement"),
		}
	}

	fn extract_apply(stmt: crate::ast::Statement<'_>) -> &ApplyExpr<'_> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Apply(a) => a,
			_ => panic!("Expected APPLY expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_apply_counter_no_args() {
		let bump = Bump::new();
		let source = "APPLY counter {}";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let a = extract_apply(stmt);
		assert_eq!(a.operator, "counter");
		assert_eq!(a.expressions.len(), 0);
	}

	#[test]
	fn test_apply_with_single_expression() {
		let bump = Bump::new();
		let source = "APPLY transform { value }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let a = extract_apply(stmt);
		assert_eq!(a.operator, "transform");
		assert_eq!(a.expressions.len(), 1);
	}

	#[test]
	fn test_apply_with_block() {
		let bump = Bump::new();
		let source = "APPLY process { a, b, c }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let a = extract_apply(stmt);
		assert_eq!(a.operator, "process");
		assert_eq!(a.expressions.len(), 3);
	}

	#[test]
	fn test_apply_lowercase() {
		let bump = Bump::new();
		let source = "apply counter {}";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let a = extract_apply(stmt);
		assert_eq!(a.operator, "counter");
	}

	#[test]
	fn test_apply_in_pipeline() {
		let bump = Bump::new();
		let source = "FROM test.orders | APPLY summarize {}";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			crate::ast::Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 2);
				match &p.stages[1] {
					Expr::Apply(a) => {
						assert_eq!(a.operator, "summarize");
					}
					_ => panic!("Expected APPLY expression"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}
}
