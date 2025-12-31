// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Parser for the unified RQL AST.
//!
//! This module implements a Pratt parser that produces bump-allocated AST nodes.

mod error;
mod pratt;
mod primary;
mod query;

use bumpalo::{Bump, collections::Vec as BumpVec};
pub use error::{ParseError, ParseErrorKind};
pub use pratt::Precedence;

use super::{Program, Statement};
use crate::token::{EOF_TOKEN, Keyword, Operator, Punctuation, Span, Token, TokenKind};

/// Parse result.
pub struct ParseResult<'bump> {
	pub program: Program<'bump>,
	pub errors: &'bump [ParseError],
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
			if self.is_eof() {
				break;
			}

			match self.parse_statement() {
				Ok(stmt) => statements.push(stmt),
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

			// Query pipeline or expression
			_ => self.parse_pipeline_or_expr(),
		}
	}

	/// Parse let statement.
	fn parse_let(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Let)?;

		// Check for mutable
		let mutable = self.try_consume_keyword(Keyword::Mut);

		// Expect variable
		if !matches!(self.current().kind, TokenKind::Variable) {
			return Err(self.error(ParseErrorKind::ExpectedVariable));
		}

		let var_token = self.advance();
		let name = self.token_text(&var_token);
		// Strip the $ prefix
		let name = if name.starts_with('$') {
			&name[1..]
		} else {
			name
		};
		let name = self.alloc_str(name);

		// Expect :=
		self.expect_operator(Operator::ColonEqual)?;

		// Parse value (expression or pipeline)
		let value = self.parse_let_value()?;

		let span = start_span.merge(&value.span());

		Ok(Statement::Let(super::stmt::LetStmt::new(name, value, mutable, span)))
	}

	/// Parse the value part of a let statement.
	fn parse_let_value(&mut self) -> Result<super::stmt::LetValue<'bump>, ParseError> {
		// Parse first expression
		let first = self.parse_expr(Precedence::None)?;

		// Check for pipe to make it a pipeline
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

			Ok(super::stmt::LetValue::Pipeline(stages.into_bump_slice()))
		} else {
			Ok(super::stmt::LetValue::Expr(first))
		}
	}

	/// Parse if statement.
	fn parse_if_stmt(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::If)?;

		let condition = self.parse_expr(Precedence::None)?;

		self.expect_punct(Punctuation::OpenCurly)?;
		let then_branch = self.parse_block()?;
		let mut end_span = self.expect_punct(Punctuation::CloseCurly)?;

		// Parse else-if branches
		let mut else_ifs = BumpVec::new_in(self.bump);
		while self.try_consume_keyword(Keyword::Else) {
			if self.try_consume_keyword(Keyword::If) {
				let cond = self.parse_expr(Precedence::None)?;
				self.expect_punct(Punctuation::OpenCurly)?;
				let body = self.parse_block()?;
				end_span = self.expect_punct(Punctuation::CloseCurly)?;

				else_ifs.push(super::stmt::ElseIfBranch::new(cond, body, cond.span().merge(&end_span)));
			} else {
				// else branch
				self.expect_punct(Punctuation::OpenCurly)?;
				let else_body = self.parse_block()?;
				end_span = self.expect_punct(Punctuation::CloseCurly)?;

				return Ok(Statement::If(super::stmt::IfStmt::new(
					condition,
					then_branch,
					else_ifs.into_bump_slice(),
					Some(else_body),
					start_span.merge(&end_span),
				)));
			}
		}

		Ok(Statement::If(super::stmt::IfStmt::new(
			condition,
			then_branch,
			else_ifs.into_bump_slice(),
			None,
			start_span.merge(&end_span),
		)))
	}

	/// Parse loop statement.
	fn parse_loop(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Loop)?;

		self.expect_punct(Punctuation::OpenCurly)?;
		let body = self.parse_block()?;
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(Statement::Loop(super::stmt::LoopStmt::new(body, start_span.merge(&end_span))))
	}

	/// Parse for statement.
	fn parse_for(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::For)?;

		// Expect variable
		if !matches!(self.current().kind, TokenKind::Variable) {
			return Err(self.error(ParseErrorKind::ExpectedVariable));
		}

		let var_token = self.advance();
		let name = self.token_text(&var_token);
		let name = if name.starts_with('$') {
			&name[1..]
		} else {
			name
		};
		let name = self.alloc_str(name);

		self.expect_keyword(Keyword::In)?;

		let iterable = self.parse_expr(Precedence::None)?;

		self.expect_punct(Punctuation::OpenCurly)?;
		let body = self.parse_block()?;
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(Statement::For(super::stmt::ForStmt::new(name, iterable, body, start_span.merge(&end_span))))
	}

	/// Parse def statement.
	fn parse_def(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Fn)?;

		// Expect function name
		if !matches!(self.current().kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}

		let name_token = self.advance();
		let name = self.token_text(&name_token);

		// Parse parameters
		self.expect_punct(Punctuation::OpenParen)?;
		let params = self.parse_parameters()?;
		self.expect_punct(Punctuation::CloseParen)?;

		// Parse body
		self.expect_punct(Punctuation::OpenCurly)?;
		let body = self.parse_block()?;
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(Statement::Def(super::stmt::DefStmt::new(name, params, body, start_span.merge(&end_span))))
	}

	/// Parse function parameters.
	fn parse_parameters(&mut self) -> Result<&'bump [super::stmt::Parameter<'bump>], ParseError> {
		let mut params = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseParen) {
			if !matches!(self.current().kind, TokenKind::Variable) {
				return Err(self.error(ParseErrorKind::ExpectedVariable));
			}

			let name_token = self.advance();
			let name = self.token_text(&name_token);
			let span = name_token.span;

			// Optional type annotation
			let param_type = if self.try_consume_operator(Operator::Colon) {
				if !matches!(self.current().kind, TokenKind::Identifier) {
					return Err(self.error(ParseErrorKind::ExpectedIdentifier));
				}
				let type_token = self.advance();
				Some(self.token_text(&type_token))
			} else {
				None
			};

			params.push(super::stmt::Parameter::new(name, param_type, span));

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		Ok(params.into_bump_slice())
	}

	/// Parse return statement.
	fn parse_return(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Return)?;

		// Optional return value
		let value = if self.is_at_statement_end() {
			None
		} else {
			Some(self.parse_expr(Precedence::None)?)
		};

		let span = match &value {
			Some(v) => start_span.merge(&v.span()),
			None => start_span,
		};

		Ok(Statement::Return(super::stmt::ReturnStmt::new(value, span)))
	}

	/// Parse break statement.
	fn parse_break(&mut self) -> Result<Statement<'bump>, ParseError> {
		let span = self.expect_keyword(Keyword::Break)?;
		Ok(Statement::Break(super::stmt::BreakStmt::new(span)))
	}

	/// Parse continue statement.
	fn parse_continue(&mut self) -> Result<Statement<'bump>, ParseError> {
		let span = self.expect_keyword(Keyword::Continue)?;
		Ok(Statement::Continue(super::stmt::ContinueStmt::new(span)))
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
			let span = first.span();
			Ok(Statement::Expression(super::stmt::ExprStmt::new(first, span)))
		}
	}

	// === DDL/DML Stubs ===

	fn parse_create(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement CREATE parsing
		Err(self.error(ParseErrorKind::NotImplemented("CREATE")))
	}

	fn parse_alter(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement ALTER parsing
		Err(self.error(ParseErrorKind::NotImplemented("ALTER")))
	}

	fn parse_drop(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement DROP parsing
		Err(self.error(ParseErrorKind::NotImplemented("DROP")))
	}

	fn parse_insert(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement INSERT parsing
		Err(self.error(ParseErrorKind::NotImplemented("INSERT")))
	}

	fn parse_update(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement UPDATE parsing
		Err(self.error(ParseErrorKind::NotImplemented("UPDATE")))
	}

	fn parse_delete(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement DELETE parsing
		Err(self.error(ParseErrorKind::NotImplemented("DELETE")))
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
