// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Parser for the unified RQL AST.
//!
//! This module implements a Pratt parser that produces bump-allocated AST nodes.

pub mod aggregate;
pub mod apply;
pub mod ddl;
pub mod distinct;
pub mod dml;
pub mod error;
pub mod extend;
pub mod filter;
pub mod from;
pub mod join;
pub mod map;
pub mod merge;
pub mod namespace;
pub mod pratt;
pub mod primary;
pub mod sort;
pub mod stmt;
pub mod take;
pub mod window;

use bumpalo::{Bump, collections::Vec as BumpVec};
use error::{ParseError, ParseErrorKind};
use pratt::Precedence;

use super::{Expr, Program, Statement};
use crate::{
	ast::{expr::operator::BinaryOp, stmt::binding::AssignStmt},
	error::RqlError,
	token::{
		EOF_TOKEN,
		keyword::Keyword,
		operator::Operator,
		punctuation::Punctuation,
		span::Span,
		token::{Token, TokenKind},
	},
};

/// Parse result.
pub struct ParseResult<'bump> {
	pub program: Program<'bump>,
	pub errors: &'bump [ParseError],
}

/// Parse RQL source into a Program AST.
///
/// # Arguments
///
/// * `bump` - The bump allocator for AST nodes
/// * `tokens` - The token stream from lexer
/// * `source` - The original source code
///
/// # Returns
///
/// A `Program` AST node, or an `RqlError` if parsing fails.
pub fn parse<'bump>(
	bump: &'bump Bump,
	tokens: &'bump [crate::token::token::Token],
	source: &str,
) -> Result<Program<'bump>, RqlError> {
	let parse_result = Parser::new(bump, tokens, source).parse();

	if !parse_result.errors.is_empty() {
		return Err(RqlError::Parse(parse_result.errors.to_vec()));
	}

	Ok(parse_result.program)
}

/// Parser for RQL v2.
pub struct Parser<'bump, 'src> {
	/// The bump allocator for AST nodes.
	bump: &'bump Bump,
	/// Token stream from lexer.
	tokens: &'bump [Token],
	/// Original source for extracting text.
	source: &'src str,
	/// Current position in token stream.
	position: usize,
}

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Create a new parser.
	pub fn new(bump: &'bump Bump, tokens: &'bump [Token], source: &'src str) -> Self {
		Self {
			bump,
			tokens,
			source,
			position: 0,
		}
	}

	/// Parse the entire program.
	pub fn parse(mut self) -> ParseResult<'bump> {
		let mut statements = BumpVec::new_in(self.bump);
		let mut errors = BumpVec::new_in(self.bump);

		while !self.is_eof() {
			self.skip_newlines();

			// Skip empty statements (multiple consecutive semicolons)
			while self.try_consume_punct(Punctuation::Semicolon) {
				self.skip_newlines();
			}

			if self.is_eof() {
				break;
			}

			match self.parse_statement() {
				Ok(stmt) => {
					statements.push(stmt);
					// Optional statement separator (semicolon)
					self.try_consume_punct(Punctuation::Semicolon);
				}
				Err(e) => {
					errors.push(e);
					self.synchronize();
				}
			}
		}

		let span = if statements.is_empty() {
			Span::default()
		} else {
			let first = statements.first().unwrap().span();
			let last = statements.last().unwrap().span();
			first.merge(&last)
		};

		ParseResult {
			program: Program::new(statements.into_bump_slice(), span),
			errors: errors.into_bump_slice(),
		}
	}

	// === Token Navigation ===

	/// Check if we're at the end of the token stream.
	#[inline]
	fn is_eof(&self) -> bool {
		self.position >= self.tokens.len() || self.current().kind == TokenKind::Eof
	}

	/// Get the current token.
	#[inline]
	fn current(&self) -> &Token {
		self.tokens.get(self.position).unwrap_or(&EOF_TOKEN)
	}

	/// Peek at the next token.
	#[inline]
	fn peek(&self) -> &Token {
		self.tokens.get(self.position + 1).unwrap_or(&EOF_TOKEN)
	}

	/// Advance to the next token, returning a copy of the current token.
	#[inline]
	fn advance(&mut self) -> Token {
		let token = *self.current();
		if !self.is_eof() {
			self.position += 1;
		}
		token
	}

	/// Skip newline tokens.
	fn skip_newlines(&mut self) {
		while matches!(self.current().kind, TokenKind::Punctuation(Punctuation::Newline)) {
			self.advance();
		}
	}

	/// Check if current token matches a keyword.
	#[inline]
	fn check_keyword(&self, kw: Keyword) -> bool {
		self.current().kind == TokenKind::Keyword(kw)
	}

	/// Check if current token matches an operator.
	#[inline]
	fn check_operator(&self, op: Operator) -> bool {
		self.current().kind == TokenKind::Operator(op)
	}

	/// Check if current token matches a punctuation.
	#[inline]
	fn check_punct(&self, p: Punctuation) -> bool {
		self.current().kind == TokenKind::Punctuation(p)
	}

	/// Try to consume a keyword, returning true if matched.
	fn try_consume_keyword(&mut self, kw: Keyword) -> bool {
		if self.check_keyword(kw) {
			self.advance();
			true
		} else {
			false
		}
	}

	/// Try to consume an operator, returning true if matched.
	fn try_consume_operator(&mut self, op: Operator) -> bool {
		if self.check_operator(op) {
			self.advance();
			true
		} else {
			false
		}
	}

	/// Try to consume a punctuation, returning true if matched.
	fn try_consume_punct(&mut self, p: Punctuation) -> bool {
		if self.check_punct(p) {
			self.advance();
			true
		} else {
			false
		}
	}

	/// Expect a keyword, returning an error if not matched.
	fn expect_keyword(&mut self, kw: Keyword) -> Result<Span, ParseError> {
		if self.check_keyword(kw) {
			Ok(self.advance().span)
		} else {
			Err(self.error(ParseErrorKind::ExpectedKeyword(kw)))
		}
	}

	/// Expect a punctuation, returning an error if not matched.
	fn expect_punct(&mut self, p: Punctuation) -> Result<Span, ParseError> {
		if self.check_punct(p) {
			Ok(self.advance().span)
		} else {
			Err(self.error(ParseErrorKind::ExpectedPunctuation(p)))
		}
	}

	/// Synchronize after an error by skipping to next statement.
	fn synchronize(&mut self) {
		while !self.is_eof() {
			// Stop at statement boundaries
			if matches!(
				self.current().kind,
				TokenKind::Punctuation(Punctuation::Semicolon)
					| TokenKind::Punctuation(Punctuation::Newline)
			) {
				self.advance();
				return;
			}

			// Stop before statement-starting keywords
			if matches!(
				self.current().kind,
				TokenKind::Keyword(Keyword::Let)
					| TokenKind::Keyword(Keyword::If) | TokenKind::Keyword(Keyword::For)
					| TokenKind::Keyword(Keyword::Loop) | TokenKind::Keyword(Keyword::Fn)
					| TokenKind::Keyword(Keyword::Return)
					| TokenKind::Keyword(Keyword::Create)
					| TokenKind::Keyword(Keyword::Alter)
					| TokenKind::Keyword(Keyword::Drop) | TokenKind::Keyword(Keyword::Insert)
					| TokenKind::Keyword(Keyword::Update)
					| TokenKind::Keyword(Keyword::Delete)
			) {
				return;
			}

			self.advance();
		}
	}

	// === Allocation Helpers ===

	/// Allocate a value in the bump arena.
	#[inline]
	fn alloc<T>(&self, value: T) -> &'bump T {
		self.bump.alloc(value)
	}

	/// Allocate a string in the bump arena.
	#[inline]
	fn alloc_str(&self, s: &str) -> &'bump str {
		self.bump.alloc_str(s)
	}

	/// Get text for a token from the source.
	fn token_text(&self, token: &Token) -> &'bump str {
		let text = token.span.text(self.source);
		self.alloc_str(text)
	}

	// === Error Helpers ===

	/// Create a parse error at current position.
	fn error(&self, kind: ParseErrorKind) -> ParseError {
		ParseError {
			kind,
			span: self.current().span,
		}
	}

	// === Statement Parsing ===

	/// Parse a single statement.
	fn parse_statement(&mut self) -> Result<Statement<'bump>, ParseError> {
		self.skip_newlines();

		let token = self.current();

		match token.kind {
			// Control flow
			TokenKind::Keyword(Keyword::Let) => self.parse_let(),
			TokenKind::Keyword(Keyword::If) => self.parse_if_stmt(),
			TokenKind::Keyword(Keyword::Loop) => self.parse_loop(),
			TokenKind::Keyword(Keyword::For) => self.parse_for(),
			TokenKind::Keyword(Keyword::Fn) => self.parse_def(),
			TokenKind::Keyword(Keyword::Return) => self.parse_return(),
			TokenKind::Keyword(Keyword::Break) => self.parse_break(),
			TokenKind::Keyword(Keyword::Continue) => self.parse_continue(),

			// DDL
			TokenKind::Keyword(Keyword::Create) => self.parse_create(),
			TokenKind::Keyword(Keyword::Alter) => self.parse_alter(),
			TokenKind::Keyword(Keyword::Drop) => self.parse_drop(),

			// DML
			TokenKind::Keyword(Keyword::Insert) => self.parse_insert(),
			TokenKind::Keyword(Keyword::Update) => self.parse_update(),
			TokenKind::Keyword(Keyword::Delete) => self.parse_delete(),

			// Utility
			TokenKind::Keyword(Keyword::Describe) => self.parse_describe(),

			// Query pipeline or expression
			_ => self.parse_pipeline_or_expr(),
		}
	}

	/// Parse a block of statements.
	fn parse_block(&mut self) -> Result<&'bump [Statement<'bump>], ParseError> {
		let mut statements = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			self.skip_newlines();
			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let stmt = self.parse_statement()?;
			statements.push(stmt);

			// Optional statement separator
			self.try_consume_punct(Punctuation::Semicolon);
		}

		Ok(statements.into_bump_slice())
	}

	/// Check if we're at a statement end.
	fn is_at_statement_end(&self) -> bool {
		matches!(
			self.current().kind,
			TokenKind::Eof
				| TokenKind::Punctuation(Punctuation::Semicolon)
				| TokenKind::Punctuation(Punctuation::Newline)
				| TokenKind::Punctuation(Punctuation::CloseCurly)
		)
	}

	/// Parse pipeline or expression statement.
	fn parse_pipeline_or_expr(&mut self) -> Result<Statement<'bump>, ParseError> {
		let first = self.parse_expr(Precedence::None)?;

		// Check for pipe operators
		if self.try_consume_operator(Operator::Pipe) {
			let mut stages = BumpVec::new_in(self.bump);
			stages.push(*first);

			loop {
				let stage = self.parse_expr(Precedence::None)?;
				stages.push(*stage);

				if !self.try_consume_operator(Operator::Pipe) {
					break;
				}
			}

			let span = stages.first().unwrap().span().merge(&stages.last().unwrap().span());
			Ok(Statement::Pipeline(super::Pipeline::new(stages.into_bump_slice(), span)))
		} else {
			// Check if this is an assignment expression ($var = expr)
			if let Expr::Binary(bin) = first {
				if bin.op == BinaryOp::Assign {
					if let Expr::Variable(var) = bin.left {
						return Ok(Statement::Assign(AssignStmt::new(
							var.name, bin.right, bin.span,
						)));
					}
				}
			}
			let span = first.span();
			Ok(Statement::Expression(super::stmt::ExprStmt::new(first, span)))
		}
	}

	/// Expect an operator.
	fn expect_operator(&mut self, op: Operator) -> Result<Span, ParseError> {
		if self.check_operator(op) {
			Ok(self.advance().span)
		} else {
			Err(self.error(ParseErrorKind::ExpectedOperator(op)))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			Expr, Statement,
			expr::{literal::Literal, operator::BinaryOp, query::FromExpr},
		},
		token::tokenize,
	};

	#[test]
	fn test_pipe_operator_simple() {
		let bump = Bump::new();
		let source = "FROM users | SORT { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		// Should be a single pipeline statement
		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 2);
				// Verify FROM source name
				match &p.stages[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.name, "users");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify SORT has a sort column
				match &p.stages[1] {
					Expr::Sort(sort) => {
						assert_eq!(sort.columns.len(), 1);
					}
					_ => panic!("Expected SORT"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}

	#[test]
	fn test_pipe_operator_multiple() {
		let bump = Bump::new();
		let source = "FROM users | FILTER age > 18 | SORT { name } | TAKE 10";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 4);
				// Verify FROM
				match &p.stages[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.name, "users");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify FILTER has predicate with "age"
				match &p.stages[1] {
					Expr::Filter(f) => match f.predicate {
						Expr::Binary(b) => {
							assert_eq!(b.op, BinaryOp::Gt);
						}
						_ => panic!("Expected Binary predicate"),
					},
					_ => panic!("Expected FILTER"),
				}
				// Verify SORT
				match &p.stages[2] {
					Expr::Sort(sort) => {
						assert_eq!(sort.columns.len(), 1);
					}
					_ => panic!("Expected SORT"),
				}
				// Verify TAKE
				match &p.stages[3] {
					Expr::Take(take) => match take.count {
						Expr::Literal(Literal::Integer {
							value,
							..
						}) => {
							assert_eq!(*value, "10");
						}
						_ => panic!("Expected integer literal"),
					},
					_ => panic!("Expected TAKE"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}

	#[test]
	fn test_pipe_with_qualified_table() {
		let bump = Bump::new();
		let source = "FROM system.tables | SORT { id }";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 2);
				// Verify qualified FROM
				match &p.stages[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.namespace, Some("system"));
						assert_eq!(s.name, "tables");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify SORT
				match &p.stages[1] {
					Expr::Sort(sort) => {
						assert_eq!(sort.columns.len(), 1);
					}
					_ => panic!("Expected SORT"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}

	#[test]
	fn test_semicolon_statement_separation() {
		let bump = Bump::new();
		let source = "let $x = 1; FROM users";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 2);
		// Verify first statement is LET with variable name
		match program.statements[0] {
			Statement::Let(l) => {
				assert_eq!(l.name, "x");
			}
			_ => panic!("Expected LET statement"),
		}

		// Second statement is an expression/pipeline with FROM
		match program.statements[1] {
			Statement::Expression(e) => match e.expr {
				Expr::From(FromExpr::Source(s)) => {
					assert_eq!(s.name, "users");
				}
				_ => panic!("Expected FROM Source"),
			},
			Statement::Pipeline(p) => match &p.stages[0] {
				Expr::From(FromExpr::Source(s)) => {
					assert_eq!(s.name, "users");
				}
				_ => panic!("Expected FROM Source"),
			},
			_ => panic!("Expected Expression or Pipeline statement"),
		}
	}

	#[test]
	fn test_between_expression() {
		let bump = Bump::new();
		let source = "x BETWEEN 1 AND 10";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Expression(e) => {
				match e.expr {
					Expr::Between(b) => {
						// Verify value is identifier "x"
						match b.value {
							Expr::Identifier(id) => {
								assert_eq!(id.name, "x");
							}
							_ => panic!("Expected identifier"),
						}
						// Verify lower bound
						match b.lower {
							Expr::Literal(Literal::Integer {
								value,
								..
							}) => {
								assert_eq!(*value, "1");
							}
							_ => panic!("Expected integer literal"),
						}
						// Verify upper bound
						match b.upper {
							Expr::Literal(Literal::Integer {
								value,
								..
							}) => {
								assert_eq!(*value, "10");
							}
							_ => panic!("Expected integer literal"),
						}
					}
					_ => panic!("Expected BETWEEN expression"),
				}
			}
			_ => panic!("Expected Expression statement"),
		}
	}

	#[test]
	fn test_in_expression() {
		let bump = Bump::new();
		let source = "x IN [1, 2, 3]";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Expression(e) => {
				match e.expr {
					Expr::In(in_expr) => {
						// Verify value is identifier "x"
						match in_expr.value {
							Expr::Identifier(id) => {
								assert_eq!(id.name, "x");
							}
							_ => panic!("Expected identifier"),
						}
						// Verify list has 3 elements
						match in_expr.list {
							Expr::List(l) => {
								assert_eq!(l.elements.len(), 3);
							}
							_ => panic!("Expected list"),
						}
						assert!(!in_expr.negated);
					}
					_ => panic!("Expected IN expression"),
				}
			}
			_ => panic!("Expected Expression statement"),
		}
	}

	#[test]
	fn test_not_in_expression() {
		let bump = Bump::new();
		let source = "x NOT IN [1, 2, 3]";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Expression(e) => {
				if let Expr::In(in_expr) = e.expr {
					assert!(in_expr.negated);
				} else {
					panic!("Expected In expression");
				}
			}
			_ => panic!("Expected Expression statement"),
		}
	}

	#[test]
	fn test_single_expression() {
		let bump = Bump::new();
		let source = "1 + 2 * 3";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Expression(e) => {
				match e.expr {
					Expr::Binary(b) => {
						// Due to precedence, this is (1 + (2 * 3))
						assert_eq!(b.op, BinaryOp::Add);
						match b.left {
							Expr::Literal(Literal::Integer {
								value,
								..
							}) => {
								assert_eq!(*value, "1");
							}
							_ => panic!("Expected integer literal"),
						}
						match b.right {
							Expr::Binary(inner) => {
								assert_eq!(inner.op, BinaryOp::Mul);
							}
							_ => panic!("Expected Binary on right"),
						}
					}
					_ => panic!("Expected Binary expression"),
				}
			}
			_ => panic!("Expected Expression statement"),
		}
	}

	#[test]
	fn test_function_call() {
		let bump = Bump::new();
		let source = "count(*)";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Expression(e) => {
				match e.expr {
					Expr::Call(c) => {
						// Verify function name
						match c.function {
							Expr::Identifier(id) => {
								assert_eq!(id.name, "count");
							}
							_ => panic!("Expected function identifier"),
						}
						// Verify single wildcard argument
						assert_eq!(c.arguments.len(), 1);
						match &c.arguments[0] {
							Expr::Wildcard(_) => {}
							_ => panic!("Expected wildcard argument"),
						}
					}
					_ => panic!("Expected Call expression"),
				}
			}
			_ => panic!("Expected Expression statement"),
		}
	}

	#[test]
	fn test_multiple_statements() {
		let bump = Bump::new();
		let source = "let $a = 1; let $b = 2; let $c = $a + $b";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 3);
		// Verify variable names
		match program.statements[0] {
			Statement::Let(l) => assert_eq!(l.name, "a"),
			_ => panic!("Expected LET statement"),
		}
		match program.statements[1] {
			Statement::Let(l) => assert_eq!(l.name, "b"),
			_ => panic!("Expected LET statement"),
		}
		match program.statements[2] {
			Statement::Let(l) => assert_eq!(l.name, "c"),
			_ => panic!("Expected LET statement"),
		}
	}

	#[test]
	fn test_empty_program() {
		let bump = Bump::new();
		let source = "";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 0);
	}

	#[test]
	fn test_only_semicolons() {
		let bump = Bump::new();
		let source = ";;;";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 0);
	}

	#[test]
	fn test_literal_values() {
		let bump = Bump::new();

		// Integer
		let result = tokenize("42", &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, "42").unwrap();
		assert_eq!(program.statements.len(), 1);

		// Float
		let result = tokenize("3.14", &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, "3.14").unwrap();
		assert_eq!(program.statements.len(), 1);

		// String
		let result = tokenize("'hello'", &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, "'hello'").unwrap();
		assert_eq!(program.statements.len(), 1);

		// Boolean
		let result = tokenize("true", &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, "true").unwrap();
		assert_eq!(program.statements.len(), 1);
	}

	#[test]
	fn test_variable_reference() {
		let bump = Bump::new();
		let source = "$my_var";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Expression(e) => match e.expr {
				Expr::Variable(v) => {
					assert_eq!(v.name, "my_var");
				}
				_ => panic!("Expected Variable expression"),
			},
			_ => panic!("Expected Expression statement"),
		}
	}

	#[test]
	fn test_pipeline_with_filter_and_map() {
		let bump = Bump::new();
		let source = "FROM orders | FILTER total > 100 | MAP { id, total }";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Pipeline(p) => {
				assert_eq!(p.stages.len(), 3);
				// Verify FROM
				match &p.stages[0] {
					Expr::From(FromExpr::Source(s)) => {
						assert_eq!(s.name, "orders");
					}
					_ => panic!("Expected FROM Source"),
				}
				// Verify FILTER
				match &p.stages[1] {
					Expr::Filter(f) => match f.predicate {
						Expr::Binary(b) => {
							assert_eq!(b.op, BinaryOp::Gt);
							match b.left {
								Expr::Identifier(id) => {
									assert_eq!(id.name, "total");
								}
								_ => panic!("Expected identifier"),
							}
							match b.right {
								Expr::Literal(Literal::Integer {
									value,
									..
								}) => {
									assert_eq!(*value, "100");
								}
								_ => panic!("Expected integer literal"),
							}
						}
						_ => panic!("Expected Binary expression"),
					},
					_ => panic!("Expected FILTER"),
				}
				// Verify MAP
				match &p.stages[2] {
					Expr::Map(m) => {
						assert_eq!(m.projections.len(), 2);
					}
					_ => panic!("Expected MAP"),
				}
			}
			_ => panic!("Expected Pipeline statement"),
		}
	}

	#[test]
	fn test_ddl_create_table() {
		let bump = Bump::new();
		let source = "CREATE TABLE test.users { id: Int4, name: Text }";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.namespace, Some("test"));
				assert_eq!(t.name, "users");
				assert_eq!(t.columns.len(), 2);
				assert_eq!(t.columns[0].name, "id");
				assert_eq!(t.columns[0].data_type, "Int4");
				assert_eq!(t.columns[1].name, "name");
				assert_eq!(t.columns[1].data_type, "Text");
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_dml_insert() {
		let bump = Bump::new();
		// RQL v2 INSERT syntax: just INSERT table (data comes from pipeline)
		let source = "INSERT test.users";
		let result = tokenize(source, &bump).unwrap();
		let program = super::parse(&bump, &result.tokens, source).unwrap();

		assert_eq!(program.statements.len(), 1);
		match program.statements[0] {
			Statement::Insert(i) => {
				assert_eq!(i.namespace, Some("test"));
				assert_eq!(i.table, "users");
			}
			_ => panic!("Expected INSERT statement"),
		}
	}
}
