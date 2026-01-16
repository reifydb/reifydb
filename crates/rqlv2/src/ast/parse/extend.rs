// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! EXTEND expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Expr, expr::query::ExtendExpr},
	token::punctuation::Punctuation,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse EXTEND expression.
	pub(super) fn parse_extend(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume EXTEND

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut extensions = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let ext = self.parse_expr(Precedence::None)?;
			extensions.push(*ext);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Extend(ExtendExpr::new(extensions.into_bump_slice(), start_span.merge(&end_span)))))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			Expr, Statement,
			expr::{literal::Literal, query::ExtendExpr},
			parse::parse,
		},
		token::tokenize,
	};

	fn get_first_expr(stmt: Statement<'_>) -> &Expr<'_> {
		match stmt {
			Statement::Pipeline(p) => {
				assert!(!p.stages.is_empty());
				&p.stages[0]
			}
			Statement::Expression(e) => e.expr,
			_ => panic!("Expected Pipeline or Expression statement"),
		}
	}

	fn extract_extend<'a>(stmt: Statement<'a>) -> &'a ExtendExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Extend(e) => e,
			_ => panic!("Expected EXTEND expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_extend_constant_number() {
		let bump = Bump::new();
		let source = "EXTEND { 1 }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let extend = extract_extend(stmt);

		assert_eq!(extend.extensions.len(), 1);

		match &extend.extensions[0] {
			Expr::Literal(Literal::Integer {
				value,
				..
			}) => {
				assert_eq!(*value, "1");
			}
			_ => panic!("Expected integer literal"),
		}
	}

	#[test]
	fn test_extend_colon_syntax() {
		let bump = Bump::new();
		let source = "EXTEND { total: price * quantity }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let extend = extract_extend(stmt);

		assert_eq!(extend.extensions.len(), 1);
	}

	#[test]
	fn test_extend_multiple_columns() {
		let bump = Bump::new();
		let source = "EXTEND { total: price * quantity, tax: price * 0.1 }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let extend = extract_extend(stmt);

		assert_eq!(extend.extensions.len(), 2);
	}

	#[test]
	fn test_extend_lowercase() {
		let bump = Bump::new();
		let source = "extend { total: price * quantity }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let extend = extract_extend(stmt);

		assert_eq!(extend.extensions.len(), 1);
	}
}
