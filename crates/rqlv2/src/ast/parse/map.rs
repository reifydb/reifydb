// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! MAP/SELECT expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Expr, expr::*},
	token::Punctuation,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse MAP/SELECT expression.
	pub(super) fn parse_map(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume MAP or SELECT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut projections = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let proj = self.parse_expr(Precedence::None)?; // Allow AS binding
			projections.push(*proj);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Map(MapExpr::new(projections.into_bump_slice(), start_span.merge(&end_span)))))
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

	fn extract_map<'a>(stmt: crate::ast::Statement<'a>) -> &'a crate::ast::expr::MapExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Map(m) => m,
			_ => panic!("Expected MAP expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_map_constant_number() {
		let bump = Bump::new();
		let source = "MAP { 1 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);

		match &map.projections[0] {
			Expr::Literal(crate::ast::expr::Literal::Integer { value, .. }) => {
				assert_eq!(*value, "1");
			}
			_ => panic!("Expected integer literal"),
		}
	}

	#[test]
	fn test_map_multiple_expressions() {
		let bump = Bump::new();
		let source = "MAP { 1 + 2, 4 * 3 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 2);

		// First: 1 + 2
		match &map.projections[0] {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Add);
			}
			_ => panic!("Expected binary expression"),
		}

		// Second: 4 * 3
		match &map.projections[1] {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Mul);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_map_star() {
		let bump = Bump::new();
		let source = "MAP { * }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
		assert!(matches!(map.projections[0], Expr::Wildcard(_)));
	}

	#[test]
	fn test_map_keyword_as_column() {
		let bump = Bump::new();
		let source = "MAP { value }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
		match &map.projections[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "value"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_map_single_column() {
		let bump = Bump::new();
		let source = "MAP { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
		match &map.projections[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_map_multiple_columns() {
		let bump = Bump::new();
		let source = "MAP { name, age }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 2);
		match &map.projections[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}
		match &map.projections[1] {
			Expr::Identifier(id) => assert_eq!(id.name, "age"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_map_with_as_alias() {
		let bump = Bump::new();
		let source = "MAP { 1 as a }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);

		match &map.projections[0] {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::As);
				match binary.right {
					Expr::Identifier(id) => assert_eq!(id.name, "a"),
					_ => panic!("Expected identifier alias"),
				}
			}
			_ => panic!("Expected AS expression"),
		}
	}

	#[test]
	fn test_map_colon_syntax() {
		let bump = Bump::new();
		let source = "MAP { col: 1 + 2 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
	}

	#[test]
	fn test_map_colon_syntax_complex() {
		let bump = Bump::new();
		let source = "MAP { total: price * quantity }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
	}

	#[test]
	fn test_map_mixed_syntax() {
		let bump = Bump::new();
		let source = "MAP { name, total: price * quantity, age }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 3);

		// First: plain identifier
		match &map.projections[0] {
			Expr::Identifier(id) => assert_eq!(id.name, "name"),
			_ => panic!("Expected identifier"),
		}

		// Third: plain identifier
		match &map.projections[2] {
			Expr::Identifier(id) => assert_eq!(id.name, "age"),
			_ => panic!("Expected identifier"),
		}
	}

	#[test]
	fn test_map_lowercase() {
		let bump = Bump::new();
		let source = "map { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
	}

	#[test]
	fn test_select_constant_number() {
		let bump = Bump::new();
		let source = "SELECT { 1 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
	}

	#[test]
	fn test_select_star() {
		let bump = Bump::new();
		let source = "SELECT { * }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
		assert!(matches!(map.projections[0], Expr::Wildcard(_)));
	}

	#[test]
	fn test_select_columns() {
		let bump = Bump::new();
		let source = "SELECT { name, age }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 2);
	}

	#[test]
	fn test_select_lowercase() {
		let bump = Bump::new();
		let source = "select { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let map = extract_map(stmt);

		assert_eq!(map.projections.len(), 1);
	}
}
