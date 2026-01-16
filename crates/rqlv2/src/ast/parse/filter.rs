// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FILTER expression parsing.

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::ast::{Expr, expr::query::FilterExpr};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse FILTER expression.
	pub(super) fn parse_filter(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume FILTER

		let predicate = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&predicate.span());

		Ok(self.alloc(Expr::Filter(FilterExpr::new(predicate, span))))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			Expr,
			expr::{
				literal::Literal,
				operator::{BinaryOp, UnaryOp},
				query::FilterExpr,
			},
		},
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

	fn extract_filter<'a>(stmt: crate::ast::Statement<'a>) -> &'a FilterExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Filter(f) => f,
			_ => panic!("Expected FILTER expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_filter_simple_comparison() {
		let bump = Bump::new();
		let source = "FILTER price > 100";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		// Verify it's a binary expression
		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Gt);
				match binary.left {
					Expr::Identifier(id) => assert_eq!(id.name, "price"),
					_ => panic!("Expected identifier on left"),
				}
				match binary.right {
					Expr::Literal(Literal::Integer {
						..
					}) => {}
					_ => panic!("Expected number on right"),
				}
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_nested_expression() {
		let bump = Bump::new();
		let source = "FILTER (price + fee) > 100";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		// Top level should be >
		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Gt);
				// Left side should be the addition expression (parentheses for grouping only)
				match binary.left {
					Expr::Binary(inner) => {
						assert_eq!(inner.op, BinaryOp::Add);
						match inner.left {
							Expr::Identifier(id) => assert_eq!(id.name, "price"),
							_ => panic!("Expected price identifier"),
						}
						match inner.right {
							Expr::Identifier(id) => assert_eq!(id.name, "fee"),
							_ => panic!("Expected fee identifier"),
						}
					}
					Expr::Paren(inner_expr) => {
						// v2 wraps parenthesized expressions in Paren
						match inner_expr {
							Expr::Binary(inner) => {
								assert_eq!(inner.op, BinaryOp::Add);
								match inner.left {
									Expr::Identifier(id) => {
										assert_eq!(id.name, "price")
									}
									_ => panic!("Expected price identifier"),
								}
								match inner.right {
									Expr::Identifier(id) => {
										assert_eq!(id.name, "fee")
									}
									_ => panic!("Expected fee identifier"),
								}
							}
							_ => panic!("Expected binary inside paren"),
						}
					}
					other => panic!("Expected binary expression on left side, got {:?}", other),
				}
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_logical_and() {
		let bump = Bump::new();
		let source = "FILTER price > 100 and qty < 50";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		// Top level should be AND
		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::And);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_logical_or() {
		let bump = Bump::new();
		let source = "FILTER active == true or premium == true";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		// Top level should be OR
		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Or);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_complex_logical_chain() {
		let bump = Bump::new();
		let source = "FILTER active == true and price > 100 or premium == true";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		// Due to precedence: (active == true and price > 100) or (premium == true)
		// Top level should be OR
		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Or);
				// Left side should be AND
				match binary.left {
					Expr::Binary(left_binary) => {
						assert_eq!(left_binary.op, BinaryOp::And);
					}
					_ => panic!("Expected AND on left"),
				}
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_keyword_as_identifier() {
		// "value" is a keyword but can be used as column name
		let bump = Bump::new();
		let source = "FILTER value > 100";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		match filter.predicate {
			Expr::Binary(binary) => match binary.left {
				Expr::Identifier(id) => assert_eq!(id.name, "value"),
				_ => panic!("Expected identifier"),
			},
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_lowercase() {
		let bump = Bump::new();
		let source = "filter price > 100";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		match filter.predicate {
			Expr::Binary(binary) => match binary.left {
				Expr::Identifier(id) => assert_eq!(id.name, "price"),
				_ => panic!("Expected identifier"),
			},
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_not_equal() {
		let bump = Bump::new();
		let source = "FILTER status != 'inactive'";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Ne);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_less_than_or_equal() {
		let bump = Bump::new();
		let source = "FILTER age <= 65";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Le);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_greater_than_or_equal() {
		let bump = Bump::new();
		let source = "FILTER age >= 18";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		match filter.predicate {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Ge);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_filter_not_prefix() {
		let bump = Bump::new();
		let source = "FILTER not active";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let filter = extract_filter(stmt);

		match filter.predicate {
			Expr::Unary(unary) => {
				assert_eq!(unary.op, UnaryOp::Not);
				match unary.operand {
					Expr::Identifier(id) => assert_eq!(id.name, "active"),
					_ => panic!("Expected identifier operand"),
				}
			}
			_ => panic!("Expected unary expression"),
		}
	}
}
