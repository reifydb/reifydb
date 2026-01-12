// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! TAKE expression parsing.

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::ast::{Expr, expr::*};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse TAKE expression.
	pub(super) fn parse_take(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume TAKE

		let count = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&count.span());

		Ok(self.alloc(Expr::Take(TakeExpr::new(count, span))))
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

	fn extract_take<'a>(stmt: crate::ast::Statement<'a>) -> &'a crate::ast::expr::TakeExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Take(t) => t,
			_ => panic!("Expected TAKE expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_take_number() {
		let bump = Bump::new();
		let source = "TAKE 10";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let take = extract_take(stmt);

		match take.count {
			Expr::Literal(crate::ast::expr::Literal::Integer { value, .. }) => {
				assert_eq!(*value, "10");
			}
			_ => panic!("Expected integer literal"),
		}
	}

	#[test]
	fn test_take_zero() {
		let bump = Bump::new();
		let source = "TAKE 0";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let take = extract_take(stmt);

		match take.count {
			Expr::Literal(crate::ast::expr::Literal::Integer { value, .. }) => {
				assert_eq!(*value, "0");
			}
			_ => panic!("Expected integer literal"),
		}
	}

	#[test]
	fn test_take_lowercase() {
		let bump = Bump::new();
		let source = "take 5";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let take = extract_take(stmt);

		match take.count {
			Expr::Literal(crate::ast::expr::Literal::Integer { value, .. }) => {
				assert_eq!(*value, "5");
			}
			_ => panic!("Expected integer literal"),
		}
	}
}
