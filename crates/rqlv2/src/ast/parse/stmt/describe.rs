// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DESCRIBE statement parsing.
//!
//! RQL syntax: `DESCRIBE { expression }`

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		Expr, Statement,
		expr::special::SubQueryExpr,
		parse::{ParseError, Parser, Precedence},
		stmt::DescribeStmt,
	},
	token::{keyword::Keyword, operator::Operator, punctuation::Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse DESCRIBE statement.
	///
	/// Syntax: `DESCRIBE { expression }`
	///
	/// # Examples
	///
	/// ```rql
	/// DESCRIBE { MAP { cast(9924, int8) } }
	/// DESCRIBE { FROM users | FILTER active = true }
	/// ```
	pub(in crate::ast::parse) fn parse_describe(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Describe)?;

		// Expect opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		// Parse the inner expression/pipeline
		let mut stages = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let expr = self.parse_expr(Precedence::None)?;
			stages.push(*expr);

			self.skip_newlines();

			// Check for pipe operator
			if self.try_consume_operator(Operator::Pipe) {
				continue;
			}

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}
		}

		let end = self.expect_punct(Punctuation::CloseCurly)?;
		let span = start.merge(&end);

		// Wrap in SubQuery expression
		let target = self.alloc(Expr::SubQuery(SubQueryExpr::new(stages.into_bump_slice(), span)));

		Ok(Statement::Describe(DescribeStmt::new(target, span)))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			Expr, Statement,
			expr::{literal::Literal, operator::BinaryOp, query::FromExpr},
			parse::parse,
		},
		token::tokenize,
	};

	#[test]
	fn test_describe_query() {
		let bump = Bump::new();
		let source = "DESCRIBE { MAP { cast(9924, int8) } }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();

		match stmt {
			Statement::Describe(d) => {
				match d.target {
					Expr::SubQuery(sq) => {
						assert_eq!(sq.pipeline.len(), 1);
						// First stage should be MAP
						match &sq.pipeline[0] {
							Expr::Map(m) => {
								assert_eq!(m.projections.len(), 1);
								// Projection should be a function call to cast
								match &m.projections[0] {
									Expr::Call(c) => {
										match c.function {
											Expr::Identifier(id) => {
												assert_eq!(
													id.name,
													"cast"
												);
											}
											_ => panic!(
												"Expected function identifier"
											),
										}
										assert_eq!(c.arguments.len(), 2);
									}
									_ => panic!("Expected Call expression"),
								}
							}
							_ => panic!("Expected MAP expression"),
						}
					}
					_ => panic!("Expected SubQuery target"),
				}
			}
			_ => panic!("Expected DESCRIBE statement"),
		}
	}

	#[test]
	fn test_describe_with_pipeline() {
		let bump = Bump::new();
		let source = "DESCRIBE { FROM users | FILTER active == true }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();

		match stmt {
			Statement::Describe(d) => {
				match d.target {
					Expr::SubQuery(sq) => {
						assert_eq!(sq.pipeline.len(), 2);
						// First stage should be FROM
						match &sq.pipeline[0] {
							Expr::From(FromExpr::Source(s)) => {
								assert_eq!(s.name, "users");
							}
							_ => panic!("Expected FROM Source expression"),
						}
						// Second stage should be FILTER
						match &sq.pipeline[1] {
							Expr::Filter(f) => {
								// Predicate should be a binary comparison
								match f.predicate {
									Expr::Binary(b) => {
										assert_eq!(b.op, BinaryOp::Eq);
										match b.left {
											Expr::Identifier(id) => {
												assert_eq!(
													id.name,
													"active"
												);
											}
											_ => panic!(
												"Expected identifier on left"
											),
										}
									}
									_ => panic!("Expected Binary expression"),
								}
							}
							_ => panic!("Expected FILTER expression"),
						}
					}
					_ => panic!("Expected SubQuery target"),
				}
			}
			_ => panic!("Expected DESCRIBE statement"),
		}
	}

	#[test]
	fn test_describe_lowercase() {
		let bump = Bump::new();
		let source = "describe { from users }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();

		match stmt {
			Statement::Describe(d) => match d.target {
				Expr::SubQuery(sq) => {
					assert_eq!(sq.pipeline.len(), 1);
					match &sq.pipeline[0] {
						Expr::From(FromExpr::Source(s)) => {
							assert_eq!(s.name, "users");
						}
						_ => panic!("Expected FROM Source expression"),
					}
				}
				_ => panic!("Expected SubQuery target"),
			},
			_ => panic!("Expected DESCRIBE statement"),
		}
	}

	#[test]
	fn test_describe_simple_expression() {
		let bump = Bump::new();
		let source = "DESCRIBE { 1 + 2 }";
		let result = tokenize(source, &bump).unwrap();
		let program = parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();

		match stmt {
			Statement::Describe(d) => {
				match d.target {
					Expr::SubQuery(sq) => {
						assert_eq!(sq.pipeline.len(), 1);
						// First stage should be binary expression 1 + 2
						match &sq.pipeline[0] {
							Expr::Binary(b) => {
								assert_eq!(b.op, BinaryOp::Add);
								match b.left {
									Expr::Literal(Literal::Integer {
										value,
										..
									}) => {
										assert_eq!(*value, "1");
									}
									_ => panic!("Expected integer literal on left"),
								}
								match b.right {
									Expr::Literal(Literal::Integer {
										value,
										..
									}) => {
										assert_eq!(*value, "2");
									}
									_ => panic!(
										"Expected integer literal on right"
									),
								}
							}
							_ => panic!("Expected Binary expression"),
						}
					}
					_ => panic!("Expected SubQuery target"),
				}
			}
			_ => panic!("Expected DESCRIBE statement"),
		}
	}
}
