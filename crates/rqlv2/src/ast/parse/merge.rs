// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! MERGE expression parsing.
//!
//! RQL syntax: `MERGE { subquery }`

use bumpalo::collections::Vec as BumpVec;

use super::{ParseError, Parser, Precedence};
use crate::{
	ast::expr::{Expr, query::MergeExpr, special::SubQueryExpr},
	token::{keyword::Keyword, operator::Operator, punctuation::Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse MERGE expression.
	///
	/// Syntax: `MERGE { subquery }`
	///
	/// # Examples
	///
	/// ```rql
	/// MERGE { FROM test.orders }
	/// FROM source1 | MERGE { FROM source2 | FILTER active = true }
	/// ```
	pub(in crate::ast::parse) fn parse_merge(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Merge)?;

		// Expect opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		// Parse the subquery (pipeline of expressions)
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

		// Wrap in SubQuery
		let subquery = self.alloc(Expr::SubQuery(SubQueryExpr::new(stages.into_bump_slice(), span)));

		Ok(self.alloc(Expr::Merge(MergeExpr {
			subquery,
			span,
		})))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			Expr, Statement,
			expr::query::{FromExpr, MergeExpr},
		},
		token::tokenize,
	};

	fn get_first_expr<'a>(stmt: Statement<'a>) -> &'a Expr<'a> {
		match stmt {
			Statement::Pipeline(p) => {
				assert!(!p.stages.is_empty());
				&p.stages[0]
			}
			Statement::Expression(e) => e.expr,
			_ => panic!("Expected Pipeline or Expression statement"),
		}
	}

	fn extract_merge<'a>(stmt: Statement<'a>) -> &'a MergeExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Merge(m) => m,
			_ => panic!("Expected MERGE expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_merge_basic() {
		let bump = Bump::new();
		let source = "MERGE { FROM test.orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let m = extract_merge(stmt);
		match m.subquery {
			Expr::SubQuery(sq) => {
				assert_eq!(sq.pipeline.len(), 1);
				match &sq.pipeline[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.namespace, Some("test"));
						assert_eq!(s.name, "orders");
					}
					_ => panic!("Expected FROM Source"),
				}
			}
			_ => panic!("Expected SubQuery"),
		}
	}

	#[test]
	fn test_merge_with_query() {
		let bump = Bump::new();
		let source = "FROM source1 | MERGE { FROM source2 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 2);
				// Verify first stage is FROM source1
				match &p.stages[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.name, "source1");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify second stage is MERGE with FROM source2
				match &p.stages[1] {
					Expr::Merge(m) => match m.subquery {
						Expr::SubQuery(sq) => {
							assert_eq!(sq.pipeline.len(), 1);
							match &sq.pipeline[0] {
								Expr::From(FromExpr::Source(s)) => {
									assert_eq!(s.name, "source2");
								}
								_ => panic!("Expected FROM Source in subquery"),
							}
						}
						_ => panic!("Expected SubQuery"),
					},
					_ => panic!("Expected MERGE expression"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}

	#[test]
	fn test_merge_chained() {
		let bump = Bump::new();
		let source = "FROM s1 | MERGE { FROM s2 } | MERGE { FROM s3 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 3);
				// Verify first stage is FROM s1
				match &p.stages[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.name, "s1");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify second stage is MERGE with FROM s2
				match &p.stages[1] {
					Expr::Merge(m) => match m.subquery {
						Expr::SubQuery(sq) => match &sq.pipeline[0] {
							Expr::From(FromExpr::Source(s)) => {
								assert_eq!(s.name, "s2");
							}
							_ => panic!("Expected FROM Source"),
						},
						_ => panic!("Expected SubQuery"),
					},
					_ => panic!("Expected MERGE expression"),
				}
				// Verify third stage is MERGE with FROM s3
				match &p.stages[2] {
					Expr::Merge(m) => match m.subquery {
						Expr::SubQuery(sq) => match &sq.pipeline[0] {
							Expr::From(FromExpr::Source(s)) => {
								assert_eq!(s.name, "s3");
							}
							_ => panic!("Expected FROM Source"),
						},
						_ => panic!("Expected SubQuery"),
					},
					_ => panic!("Expected MERGE expression"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}

	#[test]
	fn test_merge_lowercase() {
		let bump = Bump::new();
		let source = "merge { from test.orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let m = extract_merge(stmt);
		match m.subquery {
			Expr::SubQuery(sq) => {
				assert_eq!(sq.pipeline.len(), 1);
				match &sq.pipeline[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.namespace, Some("test"));
						assert_eq!(s.name, "orders");
					}
					_ => panic!("Expected FROM Source"),
				}
			}
			_ => panic!("Expected SubQuery"),
		}
	}

	#[test]
	fn test_merge_with_filter() {
		let bump = Bump::new();
		let source = "MERGE { FROM test.orders | FILTER active == true }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let m = extract_merge(stmt);
		match m.subquery {
			Expr::SubQuery(sq) => {
				assert_eq!(sq.pipeline.len(), 2);
				// Verify FROM stage
				match &sq.pipeline[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.namespace, Some("test"));
						assert_eq!(s.name, "orders");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify FILTER stage exists
				match &sq.pipeline[1] {
					Expr::Filter(f) => {
						// Verify predicate is a binary expression
						match f.predicate {
							Expr::Binary(b) => match b.left {
								Expr::Identifier(id) => {
									assert_eq!(id.name, "active");
								}
								_ => panic!("Expected identifier"),
							},
							_ => panic!("Expected Binary expression"),
						}
					}
					_ => panic!("Expected FILTER"),
				}
			}
			_ => panic!("Expected SubQuery"),
		}
	}
}
