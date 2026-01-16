// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FROM expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, Precedence, error::ParseError};
use crate::{
	ast::{
		Expr,
		expr::query::{FromEnvironment, FromExpr, FromGenerator, FromInline, FromVariable, SourceRef},
	},
	token::{punctuation::Punctuation, token::TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse FROM expression.
	///
	/// Supports:
	/// - `FROM namespace.table` - table source
	/// - `FROM $variable` - variable source
	/// - `FROM $env` - environment source
	/// - `FROM [ { ... }, { ... } ]` - inline data
	/// - `FROM generator_name { key: value, ... }` - generator source
	pub(super) fn parse_from(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume FROM

		// Check for special cases
		if matches!(self.current().kind, TokenKind::Variable) {
			let var = self.parse_variable()?;
			if let Expr::Variable(v) = var {
				return Ok(self.alloc(Expr::From(FromExpr::Variable(FromVariable {
					variable: *v,
					span: start_span.merge(&v.span),
				}))));
			}
			if let Expr::Environment(e) = var {
				return Ok(self.alloc(Expr::From(FromExpr::Environment(FromEnvironment {
					span: e.span,
				}))));
			}
		}

		// Check for inline data: [ ... ]
		if self.check_punct(Punctuation::OpenBracket) {
			let list = self.parse_list()?;
			if let Expr::List(l) = list {
				return Ok(self.alloc(Expr::From(FromExpr::Inline(FromInline {
					rows: l.elements,
					span: start_span.merge(&l.span),
				}))));
			}
		}

		// Check if this is a generator: identifier { ... }
		// We need to peek ahead: if we have an identifier followed by {, it's a generator
		if matches!(self.current().kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			let name_token = self.current();
			let name = self.token_text(name_token);

			// Check if the next token is {
			if self.peek().kind == TokenKind::Punctuation(Punctuation::OpenCurly) {
				// This is a generator
				self.advance(); // consume the name
				let params = self.parse_generator_params()?;
				let end_span = self.current().span;

				return Ok(self.alloc(Expr::From(FromExpr::Generator(FromGenerator {
					name,
					params,
					span: start_span.merge(&end_span),
				}))));
			}

			// Not a generator, parse as qualified name
			// We need to continue from where we are (we haven't consumed the name yet)
		}

		// Parse qualified name: namespace.table or ns1::ns2.table
		let qualified = self.parse_qualified_name()?;

		Ok(self.alloc(Expr::From(FromExpr::Source(
			SourceRef::new(qualified.name, start_span.merge(&qualified.span))
				.with_namespace(qualified.namespace),
		))))
	}

	/// Parse generator parameters: `{ key: value, key2: value2 }`
	fn parse_generator_params(&mut self) -> Result<&'bump [Expr<'bump>], ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut params = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			// Parse key: value or just expression
			let expr = self.parse_expr(Precedence::None)?;
			params.push(*expr);

			self.skip_newlines();

			// Check for comma
			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		Ok(params.into_bump_slice())
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			Expr, Statement,
			expr::query::{FromExpr, FromGenerator, SourceRef},
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

	fn extract_from_generator(stmt: Statement<'_>) -> &FromGenerator<'_> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::From(FromExpr::Generator(g)) => g,
			_ => panic!("Expected FROM Generator expression, got {:?}", expr),
		}
	}

	fn extract_from_source<'a>(stmt: Statement<'a>) -> &'a SourceRef<'a> {
		let expr = get_first_expr(stmt);
		match expr {
			Expr::From(FromExpr::Source(s)) => s,
			_ => panic!("Expected FROM Source expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_from_generator_simple() {
		let bump = Bump::new();
		let source = "FROM generate_series { start: 1, end: 100 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let g = extract_from_generator(stmt);
		assert_eq!(g.name, "generate_series");
		assert_eq!(g.params.len(), 2);
	}

	#[test]
	fn test_from_generator_with_expression() {
		let bump = Bump::new();
		let source = "FROM data_loader { endpoint: '/api', timeout: 30 * 1000 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let g = extract_from_generator(stmt);
		assert_eq!(g.name, "data_loader");
		assert_eq!(g.params.len(), 2);
	}

	#[test]
	fn test_from_generator_single_param() {
		let bump = Bump::new();
		let source = "FROM range { count: 10 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let g = extract_from_generator(stmt);
		assert_eq!(g.name, "range");
		assert_eq!(g.params.len(), 1);
	}

	#[test]
	fn test_from_table_not_confused_with_generator() {
		// Make sure regular FROM table still works
		let bump = Bump::new();
		let source = "FROM test.users";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let s = extract_from_source(stmt);
		assert_eq!(s.namespace.unwrap(), "test");
		assert_eq!(s.name, "users");
	}

	#[test]
	fn test_from_generator_lowercase() {
		let bump = Bump::new();
		let source = "from generate_series { start: 1, end: 100 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let g = extract_from_generator(stmt);
		assert_eq!(g.name, "generate_series");
		assert_eq!(g.params.len(), 2);
	}

	#[test]
	fn test_from_namespace_table() {
		let bump = Bump::new();
		let source = "FROM reifydb.users";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let s = extract_from_source(stmt);
		assert_eq!(s.namespace, Some("reifydb"));
		assert_eq!(s.name, "users");
	}

	#[test]
	fn test_from_table_without_namespace() {
		let bump = Bump::new();
		let source = "FROM users";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let s = extract_from_source(stmt);
		// Single identifier parsed as table name
		assert_eq!(s.name, "users");
	}

	#[test]
	fn test_from_inline_empty() {
		let bump = Bump::new();
		let source = "FROM []";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::From(FromExpr::Inline(inline)) => {
				assert_eq!(inline.rows.len(), 0);
			}
			_ => panic!("Expected FROM Inline expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_from_inline_single_row() {
		let bump = Bump::new();
		let source = "FROM [ { field: 'value' }]";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::From(FromExpr::Inline(inline)) => {
				assert_eq!(inline.rows.len(), 1);
				// Verify first row is an inline object with one field
				match &inline.rows[0] {
					Expr::Inline(row) => {
						assert_eq!(row.fields.len(), 1);
						assert_eq!(row.fields[0].key, "field");
					}
					_ => panic!("Expected Inline expression for row"),
				}
			}
			_ => panic!("Expected FROM Inline expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_from_inline_multiple_rows() {
		let bump = Bump::new();
		let source = "FROM [ { field: 'value' }, { field: 'value2' } ]";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::From(FromExpr::Inline(inline)) => {
				assert_eq!(inline.rows.len(), 2);
				// Verify both rows
				match &inline.rows[0] {
					Expr::Inline(row) => {
						assert_eq!(row.fields.len(), 1);
						assert_eq!(row.fields[0].key, "field");
					}
					_ => panic!("Expected Inline expression for row 0"),
				}
				match &inline.rows[1] {
					Expr::Inline(row) => {
						assert_eq!(row.fields.len(), 1);
						assert_eq!(row.fields[0].key, "field");
					}
					_ => panic!("Expected Inline expression for row 1"),
				}
			}
			_ => panic!("Expected FROM Inline expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_from_inline_trailing_comma() {
		let bump = Bump::new();
		let source = "FROM [ { field: 'value' }, ]";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::From(FromExpr::Inline(inline)) => {
				assert_eq!(inline.rows.len(), 1);
			}
			_ => panic!("Expected FROM Inline expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_from_variable() {
		let bump = Bump::new();
		let source = "FROM $my_var";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::From(FromExpr::Variable(v)) => {
				assert_eq!(v.variable.name, "my_var");
			}
			_ => panic!("Expected FROM Variable expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_from_environment() {
		let bump = Bump::new();
		let source = "FROM $env";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::From(FromExpr::Environment(_)) => {}
			_ => panic!("Expected FROM Environment expression, got {:?}", expr),
		}
	}

	#[test]
	fn test_from_lowercase() {
		let bump = Bump::new();
		let source = "from users";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let s = extract_from_source(stmt);
		// Single identifier parsed as table name
		assert_eq!(s.name, "users");
	}
}
