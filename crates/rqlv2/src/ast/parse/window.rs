// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WINDOW expression parsing.
//!
//! RQL syntax: `WINDOW { aggregations } [WITH { config }] [BY { group_by }]`
//!
//! - WITH and BY clauses are optional and can appear in either order
//! - Config entries: `interval: "5m"`, `slide: "1m"`, `count: 100`, `rolling: true`

use bumpalo::collections::Vec as BumpVec;

use super::{ParseError, Parser, Precedence};
use crate::{
	ast::{Expr, expr::{WindowConfig, WindowExpr}},
	token::{Keyword, Operator, Punctuation, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse WINDOW expression.
	///
	/// Syntax: `WINDOW { aggregations } [WITH { config }] [BY { group_by }]`
	///
	/// # Examples
	///
	/// ```rql
	/// WINDOW { count(*) } WITH { interval: "5m" }
	/// WINDOW { sum(value) } WITH { count: 100 }
	/// WINDOW { avg(price) } WITH { interval: "1h", slide: "5m" } BY { category }
	/// WINDOW { count(*) } BY { user_id } WITH { rolling: true, count: 10 }
	/// ```
	pub(in crate::ast::parse) fn parse_window(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Window)?;

		// Parse aggregations: { expr, expr, ... }
		let aggregations = self.parse_window_block()?;

		let mut config = BumpVec::new_in(self.bump);
		let mut group_by = BumpVec::new_in(self.bump);

		// Parse optional WITH and BY clauses (can appear in any order)
		loop {
			if self.try_consume_keyword(Keyword::With) {
				// Parse config: { key: value, ... }
				let configs = self.parse_window_config()?;
				for c in configs {
					config.push(c);
				}
			} else if self.try_consume_keyword(Keyword::By) {
				// Parse group by: { expr, ... }
				let groups = self.parse_window_block()?;
				for g in groups {
					group_by.push(g);
				}
			} else {
				break;
			}
		}

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(self.alloc(Expr::Window(WindowExpr {
			config: config.into_bump_slice(),
			aggregations: aggregations.into_bump_slice(),
			group_by: group_by.into_bump_slice(),
			span,
		})))
	}

	/// Parse a window block: `{ expr, expr, ... }`
	fn parse_window_block(&mut self) -> Result<BumpVec<'bump, Expr<'bump>>, ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut exprs = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let expr = self.parse_expr(Precedence::None)?;
			exprs.push(*expr);

			self.skip_newlines();

			// Check for comma
			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		Ok(exprs)
	}

	/// Parse window config: `{ key: value, ... }`
	fn parse_window_config(&mut self) -> Result<BumpVec<'bump, WindowConfig<'bump>>, ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut configs = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			// Parse key: value
			// Key must be an identifier or keyword
			let key = match &self.current().kind {
				TokenKind::Identifier => {
					let token = self.current();
					let text = self.token_text(token);
					self.advance();
					text
				}
				TokenKind::Keyword(_) => {
					// Keywords like 'count' can be config keys
					let token = self.current();
					let text = self.token_text(token);
					self.advance();
					text
				}
				_ => return Err(self.error(super::ParseErrorKind::UnexpectedToken)),
			};

			self.expect_operator(Operator::Colon)?;

			let value = self.parse_expr(Precedence::None)?;

			configs.push(WindowConfig::new(key, value));

			self.skip_newlines();

			// Check for comma
			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		Ok(configs)
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

	fn extract_window<'a>(stmt: crate::ast::Statement<'a>) -> &'a crate::ast::expr::WindowExpr<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::Window(w) => w,
			_ => panic!("Expected WINDOW expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_parse_time_window() {
		let bump = Bump::new();
		let source = "WINDOW { count(*) } WITH { interval: \"5m\" }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
		assert_eq!(w.config.len(), 1);
		assert_eq!(w.config[0].key, "interval");
	}

	#[test]
	fn test_parse_count_window() {
		let bump = Bump::new();
		let source = "WINDOW { sum(value) } WITH { count: 100 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
		assert_eq!(w.config.len(), 1);
		assert_eq!(w.config[0].key, "count");
	}

	#[test]
	fn test_parse_sliding_window() {
		let bump = Bump::new();
		let source = "WINDOW { avg(price) } WITH { interval: \"1h\", slide: \"5m\" }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
		assert_eq!(w.config.len(), 2);
	}

	#[test]
	fn test_parse_grouped_window() {
		let bump = Bump::new();
		let source = "WINDOW { count(*) } WITH { interval: \"5m\" } BY { category }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
		assert_eq!(w.config.len(), 1);
		assert_eq!(w.group_by.len(), 1);
	}

	#[test]
	fn test_parse_window_by_then_with() {
		let bump = Bump::new();
		let source = "WINDOW { count(*) } BY { user_id } WITH { count: 10 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
		assert_eq!(w.group_by.len(), 1);
		assert_eq!(w.config.len(), 1);
	}

	#[test]
	fn test_parse_window_multiple_aggregations_and_grouping() {
		let bump = Bump::new();
		let source = "WINDOW { count(*), sum(amount), avg(price) } WITH { interval: \"1h\" } BY { category, region }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 3);
		assert_eq!(w.config.len(), 1);
		assert_eq!(w.group_by.len(), 2);
	}

	#[test]
	fn test_parse_rolling_count_window() {
		let bump = Bump::new();
		let source = "WINDOW { sum(value) } WITH { rolling: true, count: 10 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
		assert_eq!(w.config.len(), 2);
	}

	#[test]
	fn test_parse_rolling_time_window() {
		let bump = Bump::new();
		let source = "WINDOW { avg(value) } WITH { rolling: true, interval: \"5m\" }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
		assert_eq!(w.config.len(), 2);
	}

	#[test]
	fn test_window_lowercase() {
		let bump = Bump::new();
		let source = "window { count(*) } with { interval: \"5m\" }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let w = extract_window(stmt);
		assert_eq!(w.aggregations.len(), 1);
	}

	#[test]
	fn test_window_in_pipeline() {
		let bump = Bump::new();
		let source = "FROM test.events | WINDOW { count(*) } WITH { interval: \"1h\" }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			crate::ast::Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 2);
				// Verify first stage is FROM
				match &p.stages[0] {
					Expr::From(crate::ast::expr::FromExpr::Source(s)) => {
						assert_eq!(s.namespace, Some("test"));
						assert_eq!(s.name, "events");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify second stage is WINDOW with proper config
				match &p.stages[1] {
					Expr::Window(w) => {
						assert_eq!(w.aggregations.len(), 1);
						assert_eq!(w.config.len(), 1);
						assert_eq!(w.config[0].key, "interval");
					}
					_ => panic!("Expected WINDOW expression"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}
}
