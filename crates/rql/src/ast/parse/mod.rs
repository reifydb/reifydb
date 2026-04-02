// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod aggregate;
pub mod alter;
pub mod append;
pub mod apply;
pub mod assert;
pub mod authentication;
pub mod block;
pub mod call;
pub mod cast;
pub mod closure;
pub mod conditional;
pub mod create;
pub mod create_index;
pub mod def_function;
pub mod delete;
pub mod describe;
pub mod dispatch;
pub mod distinct;
pub mod drop;
pub mod extend;
pub mod filter;
pub mod from;
pub mod gate;
pub mod grant;
pub mod identifier;
pub mod identity;
pub mod infix;
pub mod inline;
pub mod insert;
pub mod join;
pub mod r#let;
pub mod list;
pub mod literal;
pub mod loop_construct;
pub mod map;
pub mod match_expr;
pub mod migrate;
pub mod patch;
pub mod policy;
pub mod prefix;
pub mod primary;
pub mod sink;
pub mod sort;
pub mod source;
pub mod sub_query;
pub mod take;
pub mod tuple;
pub mod update;
pub mod window;

use std::cmp::PartialOrd;

use Operator::*;
use Separator::NewLine;

use crate::{
	Result,
	ast::{
		ast::{
			Ast, AstBetween, AstInfix, AstInline, AstIsVariant, AstStatement, AstSumTypeConstructor,
			InfixOperator,
		},
		parse::Precedence::{Assignment, Call, Comparison, Factor, LogicAnd, LogicOr, Primary, Term},
	},
	bump::{Bump, BumpBox},
	diagnostic::AstError,
	error::{OperationKind, RqlError},
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Literal, Token, TokenKind},
	},
};

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub(crate) enum Precedence {
	None,
	Assignment,
	LogicOr,
	LogicAnd,
	Comparison,
	Term,
	Factor,
	Prefix,
	Call,
	Primary,
}

const fn get_precedence_for_operator(op: Operator) -> Precedence {
	match op {
		As => Assignment,
		Equal => Assignment,
		DoubleEqual | BangEqual | LeftAngle | LeftAngleEqual | RightAngle | RightAngleEqual => Comparison,
		Plus | Minus => Term,
		Asterisk | Slash | Percent => Factor,
		OpenParen => Call,
		Dot | DoubleColon => Primary,
		Colon => Assignment,
		Or | Xor => LogicOr,
		And => LogicAnd,
		_ => Precedence::None,
	}
}

pub fn parse<'bump>(
	bump: &'bump Bump,
	source: &'bump str,
	tokens: Vec<Token<'bump>>,
) -> Result<Vec<AstStatement<'bump>>> {
	let mut parser = Parser::new(bump, source, tokens);
	parser.parse()
}

/// Maximum nesting depth to prevent stack overflow on deeply nested input.
const MAX_PARSE_DEPTH: usize = 128;

pub(crate) struct Parser<'bump> {
	bump: &'bump Bump,
	source: &'bump str,
	tokens: Vec<Token<'bump>>,
	position: usize,
	depth: usize,
}

impl<'bump> Parser<'bump> {
	fn new(bump: &'bump Bump, source: &'bump str, tokens: Vec<Token<'bump>>) -> Self {
		Self {
			bump,
			source,
			tokens,
			position: 0,
			depth: 0,
		}
	}

	#[inline(always)]
	pub(crate) fn bump(&self) -> &'bump Bump {
		self.bump
	}

	/// Extract the source text from `start_offset` to the end of the most recently consumed token.
	pub(crate) fn source_since(&self, start_offset: usize) -> &'bump str {
		let end = if self.position > 0 {
			let prev = &self.tokens[self.position - 1];
			prev.fragment.source_end()
		} else {
			start_offset
		};
		&self.source[start_offset..end]
	}

	fn parse(&mut self) -> Result<Vec<AstStatement<'bump>>> {
		let mut result = Vec::with_capacity(4);
		loop {
			if self.is_eof() {
				break;
			}

			result.push(self.parse_statement()?);
		}
		Ok(result)
	}

	/// Parse a single statement (possibly with pipes)
	pub(crate) fn parse_statement(&mut self) -> Result<AstStatement<'bump>> {
		// Check for OUTPUT prefix
		let is_output = if !self.is_eof() && self.current()?.is_keyword(Keyword::Output) {
			self.advance()?;
			true
		} else {
			false
		};

		let mut nodes = Vec::with_capacity(8);
		let mut has_pipes = false;
		loop {
			if self.is_eof() || self.consume_if(TokenKind::Separator(Separator::Semicolon))?.is_some() {
				break;
			}

			let node = self.parse_node(Precedence::None)?;

			// Check if this is a DDL statement (CREATE, ALTER, DROP)
			// These should stand alone and not have arbitrary expressions after them
			let is_ddl = matches!(
				node,
				Ast::Create(_) | Ast::Alter(_) | Ast::Drop(_) | Ast::Grant(_) | Ast::Revoke(_)
			);

			nodes.push(node);

			if !self.is_eof() {
				// Check for pipe operator or newline as
				// separator
				if self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
				} else {
					self.consume_if(TokenKind::Separator(NewLine))?;
				}

				// If we just parsed a DDL statement, check for unexpected trailing tokens
				if is_ddl
					&& !self.is_eof() && !matches!(
					self.current()?.kind,
					TokenKind::Separator(Separator::Semicolon) | TokenKind::Separator(NewLine)
				) {
					return Err(AstError::UnexpectedToken {
						expected: "semicolon or end of statement after DDL command".to_string(),
						fragment: self.current()?.fragment.to_owned(),
					}
					.into());
				}
			}
		}

		Ok(AstStatement {
			nodes,
			has_pipes,
			is_output,
		})
	}

	/// Parse statement content without handling termination (for use within other constructs)
	pub(crate) fn parse_statement_content(&mut self) -> Result<AstStatement<'bump>> {
		let mut nodes = Vec::with_capacity(8);
		let mut has_pipes = false;
		loop {
			// Don't check for semicolon termination - that's handled by the outer context
			if self.is_eof() {
				break;
			}

			// Check if we hit a semicolon - if so, stop but don't consume it
			if let Ok(current) = self.current()
				&& current.is_separator(Separator::Semicolon)
			{
				break;
			}

			nodes.push(self.parse_node(Precedence::None)?);
			if !self.is_eof() {
				// Check for pipe operator or newline as separator
				if self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
				} else {
					self.consume_if(TokenKind::Separator(NewLine))?;
				}
			}
		}

		Ok(AstStatement {
			nodes,
			has_pipes,
			is_output: false,
		})
	}

	pub(crate) fn parse_node(&mut self, precedence: Precedence) -> Result<Ast<'bump>> {
		self.depth += 1;
		if self.depth > MAX_PARSE_DEPTH {
			self.depth -= 1;
			return Err(AstError::MaxDepthExceeded {
				fragment: self.current()?.fragment.to_owned(),
			}
			.into());
		}
		let result = self.parse_node_inner(precedence);
		self.depth -= 1;
		result
	}

	fn parse_node_inner(&mut self, precedence: Precedence) -> Result<Ast<'bump>> {
		let mut left = self.parse_primary()?;

		// DDL statements (CREATE, ALTER, DROP, GRANT, REVOKE) cannot be used in infix expressions
		// They must stand alone
		if matches!(left, Ast::Create(_) | Ast::Alter(_) | Ast::Drop(_) | Ast::Grant(_) | Ast::Revoke(_)) {
			return Ok(left);
		}

		while !self.is_eof() {
			if precedence >= self.current_precedence()? {
				break;
			}

			// Check token type before consuming
			// Use an enum to track what we found
			enum SpecialInfix {
				Between,
				In,
				NotIn,
				Is,
				Contains,
			}

			let special = if let Ok(current) = self.current() {
				match current.kind {
					TokenKind::Keyword(Keyword::Between) => Some(SpecialInfix::Between),
					TokenKind::Keyword(Keyword::In) => Some(SpecialInfix::In),
					TokenKind::Keyword(Keyword::Is) => Some(SpecialInfix::Is),
					TokenKind::Keyword(Keyword::Contains) => Some(SpecialInfix::Contains),
					TokenKind::Operator(Operator::Not) => {
						// Check if next token is IN for NOT IN
						if self.is_next_keyword(Keyword::In) {
							Some(SpecialInfix::NotIn)
						} else {
							None
						}
					}
					_ => None,
				}
			} else {
				break;
			};

			match special {
				Some(SpecialInfix::Between) => {
					left = Ast::Between(self.parse_between(left)?);
				}
				Some(SpecialInfix::In) => {
					left = Ast::Infix(self.parse_in(left, false)?);
				}
				Some(SpecialInfix::NotIn) => {
					left = Ast::Infix(self.parse_in(left, true)?);
				}
				Some(SpecialInfix::Is) => {
					left = self.parse_is(left)?;
				}
				Some(SpecialInfix::Contains) => {
					left = Ast::Infix(self.parse_contains(left)?);
				}
				_ => {
					let infix = self.parse_infix(left)?;
					if matches!(infix.operator, InfixOperator::AccessNamespace(_)) {
						if !self.is_eof()
							&& self.current()?.is_operator(Operator::OpenCurly)
							&& infix.right.is_identifier() && match infix.left.as_ref() {
							Ast::Infix(inner)
								if matches!(
									inner.operator,
									InfixOperator::AccessTable(_)
										| InfixOperator::AccessNamespace(_)
								) =>
							{
								inner.left.is_identifier()
									&& inner.right.is_identifier()
							}
							other => other.is_identifier(),
						} {
							left = self.parse_sumtype_constructor(infix)?;
							continue;
						}
						if infix.right.is_identifier()
							&& let Ast::Infix(inner) = infix.left.as_ref() && matches!(
							inner.operator,
							InfixOperator::AccessTable(_)
								| InfixOperator::AccessNamespace(_)
						) && inner.left.is_identifier() && inner.right.is_identifier()
						{
							left = self.parse_sumtype_unit_constructor(infix)?;
							continue;
						}
					}
					left = Ast::Infix(infix);
				}
			}
		}
		Ok(left)
	}

	fn parse_sumtype_constructor(&mut self, infix: AstInfix<'bump>) -> Result<Ast<'bump>> {
		let variant_name = *infix.right.as_identifier().fragment();
		let (namespace, sumtype_name) = match infix.left.as_ref() {
			Ast::Infix(inner)
				if matches!(
					inner.operator,
					InfixOperator::AccessTable(_) | InfixOperator::AccessNamespace(_)
				) =>
			{
				let ns = *inner.left.as_identifier().fragment();
				let name = *inner.right.as_identifier().fragment();
				(ns, name)
			}
			_ => {
				let name = *infix.left.as_identifier().fragment();
				(name, name)
			}
		};
		let columns = self.parse_inline()?;
		Ok(Ast::SumTypeConstructor(AstSumTypeConstructor {
			token: infix.token,
			namespace,
			sumtype_name,
			variant_name,
			columns,
		}))
	}

	fn parse_sumtype_unit_constructor(&mut self, infix: AstInfix<'bump>) -> Result<Ast<'bump>> {
		let variant_name = *infix.right.as_identifier().fragment();
		let Ast::Infix(inner) = infix.left.as_ref() else {
			unreachable!()
		};
		let namespace = *inner.left.as_identifier().fragment();
		let sumtype_name = *inner.right.as_identifier().fragment();
		Ok(Ast::SumTypeConstructor(AstSumTypeConstructor {
			token: infix.token,
			namespace,
			sumtype_name,
			variant_name,
			columns: AstInline {
				token: infix.token,
				keyed_values: vec![],
			},
		}))
	}

	pub(crate) fn advance(&mut self) -> Result<Token<'bump>> {
		if self.position >= self.tokens.len() {
			return Err(AstError::UnexpectedEof.into());
		}
		let token = self.tokens[self.position];
		self.position += 1;
		Ok(token)
	}

	pub(crate) fn consume(&mut self, expected: TokenKind) -> Result<Token<'bump>> {
		self.current_expect(expected)?;
		self.advance()
	}

	pub(crate) fn consume_if(&mut self, expected: TokenKind) -> Result<Option<Token<'bump>>> {
		if self.is_eof() || self.current()?.kind != expected {
			return Ok(None);
		}

		Ok(Some(self.consume(expected)?))
	}

	pub(crate) fn consume_while(&mut self, expected: TokenKind) -> Result<()> {
		loop {
			if self.is_eof() || self.current()?.kind != expected {
				return Ok(());
			}
			self.advance()?;
		}
	}

	pub(crate) fn consume_literal(&mut self, expected: Literal) -> Result<Token<'bump>> {
		self.current_expect_literal(expected)?;
		self.advance()
	}

	pub(crate) fn consume_operator(&mut self, expected: Operator) -> Result<Token<'bump>> {
		self.current_expect_operator(expected)?;
		self.advance()
	}

	pub(crate) fn consume_keyword(&mut self, expected: Keyword) -> Result<Token<'bump>> {
		self.current_expect_keyword(expected)?;
		self.advance()
	}

	/// Consume a token that is either an Identifier or a Keyword, returning it as an Identifier.
	/// Used in contexts where a keyword-colliding name (e.g. enum variant `Pending`) should be accepted.
	pub(crate) fn consume_name(&mut self) -> Result<Token<'bump>> {
		let token = self.advance()?;
		if matches!(token.kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
			Ok(Token {
				kind: TokenKind::Identifier,
				..token
			})
		} else {
			Err(AstError::ExpectedIdentifier {
				fragment: token.fragment.to_owned(),
			}
			.into())
		}
	}

	pub(crate) fn current(&self) -> Result<Token<'bump>> {
		if self.position >= self.tokens.len() {
			return Err(AstError::UnexpectedEof.into());
		}
		Ok(self.tokens[self.position])
	}

	/// Check if the next token (position + 1) is a specific keyword
	pub(crate) fn is_next_keyword(&self, keyword: Keyword) -> bool {
		if self.position + 1 >= self.tokens.len() {
			return false;
		}
		matches!(self.tokens[self.position + 1].kind, TokenKind::Keyword(k) if k == keyword)
	}

	pub(crate) fn current_expect(&self, expected: TokenKind) -> Result<()> {
		let got = self.current()?;
		if got.kind == expected {
			Ok(())
		} else {
			// Use specific error for identifier expectations to
			// match test format
			if let TokenKind::Identifier = expected {
				Err(AstError::ExpectedIdentifier {
					fragment: got.fragment.to_owned(),
				}
				.into())
			} else {
				Err(AstError::UnexpectedToken {
					expected: format!("{:?}", expected),
					fragment: got.fragment.to_owned(),
				}
				.into())
			}
		}
	}

	pub(crate) fn current_expect_literal(&self, literal: Literal) -> Result<()> {
		self.current_expect(TokenKind::Literal(literal))
	}

	pub(crate) fn current_expect_operator(&self, operator: Operator) -> Result<()> {
		self.current_expect(TokenKind::Operator(operator))
	}

	pub(crate) fn current_expect_keyword(&self, keyword: Keyword) -> Result<()> {
		self.current_expect(TokenKind::Keyword(keyword))
	}

	pub(crate) fn current_precedence(&self) -> Result<Precedence> {
		if self.is_eof() {
			return Ok(Precedence::None);
		};

		let current = self.current()?;
		match current.kind {
			TokenKind::Operator(operator) => {
				// Check for NOT IN (NOT followed by IN keyword)
				if operator == Operator::Not && self.is_next_keyword(Keyword::In) {
					return Ok(Precedence::Comparison);
				}
				Ok(get_precedence_for_operator(operator))
			}
			TokenKind::Keyword(Keyword::Between) => Ok(Precedence::Comparison),
			TokenKind::Keyword(Keyword::In) => Ok(Precedence::Comparison),
			TokenKind::Keyword(Keyword::Is) => Ok(Precedence::Comparison),
			TokenKind::Keyword(Keyword::Contains) => Ok(Precedence::Comparison),
			_ => Ok(Precedence::None),
		}
	}

	pub(crate) fn is_eof(&self) -> bool {
		self.position >= self.tokens.len()
	}

	/// Look ahead from current position to find a pipe operator (|)
	/// Returns true if pipe found before semicolon or EOF at depth 0
	/// Returns false if semicolon or EOF found first, or if a closing
	/// bracket/brace/paren is hit at depth 0 (we're inside a nested context)
	pub(crate) fn has_pipe_ahead(&self) -> bool {
		let mut pos = self.position;
		let mut depth = 0;

		while pos < self.tokens.len() {
			let token = &self.tokens[pos];
			match token.kind {
				TokenKind::Operator(Operator::Pipe) if depth == 0 => return true,
				TokenKind::Separator(Separator::Semicolon) if depth == 0 => return false,
				TokenKind::Operator(Operator::OpenCurly)
				| TokenKind::Operator(Operator::OpenBracket)
				| TokenKind::Operator(Operator::OpenParen) => {
					depth += 1;
				}
				TokenKind::Operator(Operator::CloseCurly)
				| TokenKind::Operator(Operator::CloseBracket)
				| TokenKind::Operator(Operator::CloseParen) => {
					if depth == 0 {
						return false;
					}
					depth -= 1;
				}
				_ => {}
			}
			pos += 1;
		}

		// Reached EOF without finding pipe or semicolon
		false
	}

	pub(crate) fn skip_new_line(&mut self) -> Result<()> {
		self.consume_while(TokenKind::Separator(NewLine))?;
		Ok(())
	}

	pub(crate) fn parse_between(&mut self, value: Ast<'bump>) -> Result<AstBetween<'bump>> {
		let token = self.consume_keyword(Keyword::Between)?;
		let lower = BumpBox::new_in(self.parse_node(Precedence::Comparison)?, self.bump());
		self.consume_operator(Operator::And)?;
		let upper = BumpBox::new_in(self.parse_node(Precedence::Comparison)?, self.bump());

		Ok(AstBetween {
			token,
			value: BumpBox::new_in(value, self.bump()),
			lower,
			upper,
		})
	}

	/// Parse an IN expression: `value IN [list]` or `value NOT IN [list]`
	pub(crate) fn parse_in(&mut self, value: Ast<'bump>, negated: bool) -> Result<AstInfix<'bump>> {
		// For NOT IN, consume NOT first
		if negated {
			self.consume_operator(Operator::Not)?;
		}
		let in_token = self.consume_keyword(Keyword::In)?;
		let right = Ast::List(self.parse_list()?);

		let operator = if negated {
			InfixOperator::NotIn(in_token)
		} else {
			InfixOperator::In(in_token)
		};

		Ok(AstInfix {
			token: *value.token(),
			left: BumpBox::new_in(value, self.bump()),
			operator,
			right: BumpBox::new_in(right, self.bump()),
		})
	}

	/// Parse a CONTAINS expression: `value CONTAINS (list)`
	pub(crate) fn parse_contains(&mut self, value: Ast<'bump>) -> Result<AstInfix<'bump>> {
		let contains_token = self.consume_keyword(Keyword::Contains)?;
		let right = Ast::List(self.parse_list()?);

		Ok(AstInfix {
			token: *value.token(),
			left: BumpBox::new_in(value, self.bump()),
			operator: InfixOperator::Contains(contains_token),
			right: BumpBox::new_in(right, self.bump()),
		})
	}

	/// Parse an IS expression: `value IS [namespace.]SumType::Variant`
	pub(crate) fn parse_is(&mut self, left: Ast<'bump>) -> Result<Ast<'bump>> {
		let is_token = self.consume_keyword(Keyword::Is)?;

		let first = self.consume_name()?;

		let (namespace, sumtype_name) = if !self.is_eof() && self.current()?.is_operator(Operator::DoubleColon)
		{
			self.consume_operator(Operator::DoubleColon)?;
			let sumtype_token = self.consume_name()?;
			(Some(first.fragment), sumtype_token.fragment)
		} else {
			(None, first.fragment)
		};

		self.consume_operator(Operator::DoubleColon)?;
		let variant_token = self.consume_name()?;

		Ok(Ast::IsVariant(AstIsVariant {
			token: is_token,
			expression: BumpBox::new_in(left, self.bump()),
			namespace,
			sumtype_name,
			variant_name: variant_token.fragment,
		}))
	}

	/// Parse a comma-separated list of expressions with optional braces
	/// Returns (nodes, had_braces) tuple
	///
	/// - `allow_colon_alias`: if true, allows `{alias: expr}` syntax which is converted to `expr AS alias`
	/// - `allow_as_keyword`: if true, allows `{expr as alias}` syntax. When false, only colon syntax is accepted.
	pub(crate) fn parse_expressions(
		&mut self,
		allow_colon_alias: bool,
		allow_as_keyword: bool,
		break_on: Option<Keyword>,
	) -> Result<(Vec<Ast<'bump>>, bool)> {
		let has_braces = self.current()?.is_operator(Operator::OpenCurly);

		if has_braces {
			self.advance()?; // consume opening brace
		}

		// Handle empty braces
		if has_braces && !self.is_eof() && self.current()?.is_operator(Operator::CloseCurly) {
			self.advance()?;
			return Ok((Vec::new(), true));
		}

		// When allow_as_keyword is false, use Assignment precedence to stop at AS
		// This allows parsing or/xor (LogicOr precedence) but stops at as (Assignment precedence)
		let precedence = if allow_as_keyword {
			Precedence::None
		} else {
			Assignment
		};

		let mut nodes = Vec::with_capacity(4);
		loop {
			// Break on keyword before parsing next expression
			if let Some(kw) = break_on
				&& !self.is_eof() && self.current()?.is_keyword(kw)
			{
				break;
			}

			if allow_colon_alias {
				if let Some(result) = self.try_parse_colon_alias() {
					nodes.push(result?);
				} else {
					nodes.push(self.parse_node(precedence)?);
				}
			} else {
				nodes.push(self.parse_node(precedence)?);
			}

			if self.is_eof() {
				break;
			}

			// consume comma and continue
			if self.current()?.is_separator(Separator::Comma) {
				self.advance()?;
			} else if has_braces && self.current()?.is_operator(Operator::CloseCurly) {
				// If we have braces, look for closing brace
				self.advance()?; // consume closing brace
				break;
			} else {
				break;
			}
		}

		Ok((nodes, has_braces))
	}

	/// Parse a keyword followed by braced, comma-separated expressions.
	/// Used by MAP, EXTEND, PATCH and similar operators that require `{ expr, ... }`.
	/// Returns `(keyword_token, parsed_expressions, rql_source)`.
	pub(crate) fn parse_keyword_with_braced_expressions(
		&mut self,
		keyword: Keyword,
		op: OperationKind,
	) -> Result<(Token<'bump>, Vec<Ast<'bump>>, &'bump str)> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(keyword)?;

		let (nodes, has_braces) = self.parse_expressions(true, false, None)?;

		if !has_braces {
			return Err(RqlError::OperatorMissingBraces {
				kind: op,
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		Ok((token, nodes, self.source_since(start)))
	}

	/// Parse a keyword followed by an optional-braces single expression.
	/// Used by FILTER, GATE and similar operators that accept `keyword { expr }` or `keyword expr`.
	/// Returns `(keyword_token, parsed_node, rql_source)`.
	pub(crate) fn parse_keyword_with_optional_braces_single(
		&mut self,
		keyword: Keyword,
	) -> Result<(Token<'bump>, BumpBox<'bump, Ast<'bump>>, &'bump str)> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(keyword)?;

		let has_braces = !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly);
		if has_braces {
			self.advance()?;
		}

		let node = if has_braces && self.current()?.is_operator(Operator::CloseCurly) {
			Ast::Nop
		} else {
			self.parse_node(Precedence::None)?
		};

		if has_braces {
			self.consume_operator(Operator::CloseCurly)?;
		}

		Ok((token, BumpBox::new_in(node, self.bump()), self.source_since(start)))
	}

	/// Fast lookahead: is the current position a `key:` colon-alias pattern?
	fn is_colon_alias(&self) -> bool {
		if self.position + 1 >= self.tokens.len() {
			return false;
		}
		let is_valid_key = self.tokens[self.position].is_identifier()
			|| matches!(self.tokens[self.position].kind, TokenKind::Keyword(_))
			|| matches!(self.tokens[self.position].kind, TokenKind::Literal(Literal::Text));
		is_valid_key && self.tokens[self.position + 1].is_operator(Operator::Colon)
	}

	/// Try to parse "key: expression" syntax and convert it to
	/// "expression AS key" where key can be identifier, keyword, or string literal.
	/// Returns `None` if the current position is not a colon-alias pattern (no error constructed).
	pub(crate) fn try_parse_colon_alias(&mut self) -> Option<Result<Ast<'bump>>> {
		if !self.is_colon_alias() {
			return None;
		}

		Some(self.parse_colon_alias_inner())
	}

	fn parse_colon_alias_inner(&mut self) -> Result<Ast<'bump>> {
		// Parse the key (identifier, keyword, or string literal)
		let key = if matches!(self.tokens[self.position].kind, TokenKind::Literal(Literal::Text)) {
			Ast::Literal(self.parse_literal_text()?)
		} else {
			Ast::Identifier(self.parse_as_identifier()?)
		};
		let colon_token = self.advance()?; // consume colon

		// Parse the expression
		let mut expression = self.parse_node(Precedence::None)?;

		// Detect simplified struct variant syntax: `Identifier { ... }`
		if let Ast::Identifier(ref ident) = expression
			&& !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly)
		{
			let token = ident.token;
			let variant_name = ident.token.fragment;
			let columns = self.parse_inline()?;
			expression = Ast::SumTypeConstructor(AstSumTypeConstructor {
				token,
				namespace: variant_name,
				sumtype_name: variant_name,
				variant_name,
				columns,
			});
		}

		// Return as "expression AS key"
		Ok(Ast::Infix(AstInfix {
			token: *expression.token(),
			left: BumpBox::new_in(expression, self.bump()),
			operator: InfixOperator::As(colon_token),
			right: BumpBox::new_in(key, self.bump()),
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstFrom},
			parse::{Parser, Precedence, Precedence::Term, parse},
		},
		bump::Bump,
		diagnostic::AstError,
		token::{
			operator::Operator::Plus,
			separator::Separator::Semicolon,
			token::{
				Literal::{False, Number, True},
				TokenKind,
				TokenKind::{Identifier, Literal, Separator},
			},
			tokenize,
		},
	};

	#[test]
	fn test_advance_but_eof() {
		let bump = Bump::new();
		let mut parser = Parser::new(&bump, "", vec![]);
		let result = parser.advance();
		assert_eq!(result, Err(AstError::UnexpectedEof.into()))
	}

	#[test]
	fn test_advance() {
		let bump = Bump::new();
		let source = "1 + 2";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);

		let one = parser.advance().unwrap();
		assert_eq!(one.kind, Literal(Number));
		assert_eq!(one.fragment.text(), "1");

		let plus = parser.advance().unwrap();
		assert_eq!(plus.kind, TokenKind::Operator(Plus));
		assert_eq!(plus.fragment.text(), "+");

		let two = parser.advance().unwrap();
		assert_eq!(two.kind, Literal(Number));
		assert_eq!(two.fragment.text(), "2");
	}

	#[test]
	fn test_consume_but_eof() {
		let bump = Bump::new();
		let source = "";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let err = parser.consume(Identifier).err().unwrap();
		assert_eq!(err, AstError::UnexpectedEof.into())
	}

	#[test]
	fn test_consume_but_unexpected_token() {
		let bump = Bump::new();
		let source = "false";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.consume(Literal(True));
		assert!(result.is_err());

		// Pattern matching no longer works with unified error system
		// Just verify it's an error for now
		assert!(result.is_err());
	}

	#[test]
	fn test_consume() {
		let bump = Bump::new();
		let source = "true 99";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.consume(Literal(True)).unwrap();
		assert_eq!(result.kind, Literal(True));

		let result = parser.consume(Literal(Number)).unwrap();
		assert_eq!(result.kind, Literal(Number));
	}

	#[test]
	fn test_consume_if_but_eof() {
		let bump = Bump::new();
		let source = "";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.consume_if(Literal(True));
		assert_eq!(result, Ok(None))
	}

	#[test]
	fn test_consume_if_but_unexpected_token() {
		let bump = Bump::new();
		let source = "false";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.consume_if(Literal(True));
		assert_eq!(result, Ok(None));
	}

	#[test]
	fn test_consume_if() {
		let bump = Bump::new();
		let source = "true 0x99";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.consume_if(Literal(True)).unwrap().unwrap();
		assert_eq!(result.kind, Literal(True));

		let result = parser.consume_if(Literal(Number)).unwrap().unwrap();
		assert_eq!(result.kind, Literal(Number));
	}

	#[test]
	fn test_current_but_eof() {
		let bump = Bump::new();
		let source = "";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let parser = Parser::new(&bump, source, tokens);
		let result = parser.current();
		assert_eq!(result, Err(AstError::UnexpectedEof.into()))
	}

	#[test]
	fn test_semicolon_statement_separation() {
		let bump = Bump::new();
		let source = "let $x = 1; FROM users";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let statements = parser.parse().unwrap();
		assert_eq!(statements.len(), 2, "Should parse two separate statements");

		// First statement should be the let assignment
		let first_stmt = &statements[0];
		assert_eq!(first_stmt.nodes.len(), 1);
		assert!(matches!(first_stmt.nodes[0], Ast::Let(_)));

		// Second statement should be the FROM
		let second_stmt = &statements[1];
		assert_eq!(second_stmt.nodes.len(), 1);
		assert!(matches!(second_stmt.nodes[0], Ast::From(_)));
	}

	#[test]
	fn test_variable_multiline_separation() {
		let bump = Bump::new();
		let sql = r#"
		let $user_data = FROM [{ name: "Alice", age: 25 }, { name: "Bob", age: 17 }, { name: "Carol", age: 30 }] | FILTER {age > 21};
		FROM $user_data
		"#;
		let tokens = tokenize(&bump, sql).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, sql, tokens);
		let statements = parser.parse().unwrap();
		assert_eq!(statements.len(), 2, "Should parse two separate statements from multiline input");

		let first_stmt = &statements[0];
		assert_eq!(first_stmt.nodes.len(), 1);
		assert!(matches!(first_stmt.nodes[0], Ast::Let(_)));

		let second_stmt = &statements[1];
		assert_eq!(second_stmt.nodes.len(), 1);
		if let Ast::From(from_ast) = &second_stmt.nodes[0] {
			assert!(matches!(from_ast, AstFrom::Variable { .. }));
		} else {
			panic!("Expected FROM statement with variable");
		}
	}

	#[test]
	fn test_current() {
		let bump = Bump::new();
		let source = "true false";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);

		let true_token = parser.current().unwrap();
		assert_eq!(true_token.kind, Literal(True));

		parser.advance().unwrap();

		let false_token = parser.current().unwrap();
		assert_eq!(false_token.kind, Literal(False));
	}

	#[test]
	fn test_current_expect_but_eof() {
		let bump = Bump::new();
		let source = "";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let parser = Parser::new(&bump, source, tokens);
		let result = parser.current_expect(Separator(Semicolon));
		assert_eq!(result, Err(AstError::UnexpectedEof.into()))
	}

	#[test]
	fn test_current_expect() {
		let bump = Bump::new();
		let source = "true false";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);

		let result = parser.current_expect(Literal(True));
		assert!(result.is_ok());

		parser.advance().unwrap();

		let result = parser.current_expect(Literal(False));
		assert!(result.is_ok());
	}

	#[test]
	fn test_current_expect_but_different() {
		let bump = Bump::new();
		let source = "true";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let parser = Parser::new(&bump, source, tokens);

		let result = parser.current_expect(Literal(False));
		assert!(result.is_err());

		// Pattern matching no longer works with unified error system
		// Just verify it's an error for now
		assert!(result.is_err());
	}

	#[test]
	fn test_current_precedence_but_eof() {
		let bump = Bump::new();
		let source = "";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let parser = Parser::new(&bump, source, tokens);
		let result = parser.current_precedence();
		assert_eq!(result, Ok(Precedence::None))
	}

	#[test]
	fn test_current_precedence() {
		let bump = Bump::new();
		let source = "+";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let parser = Parser::new(&bump, source, tokens);
		let result = parser.current_precedence();
		assert_eq!(result, Ok(Term))
	}

	#[test]
	fn test_between_precedence() {
		let bump = Bump::new();
		let source = "BETWEEN";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let parser = Parser::new(&bump, source, tokens);
		let result = parser.current_precedence();
		assert_eq!(result, Ok(Precedence::Comparison))
	}

	#[test]
	fn test_parse_between_expression() {
		let bump = Bump::new();
		let source = "x BETWEEN 1 AND 10";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let between = result[0].first_unchecked().as_between();
		assert_eq!(between.value.as_identifier().text(), "x");
		assert_eq!(between.lower.as_literal_number().value(), "1");
		assert_eq!(between.upper.as_literal_number().value(), "10");
	}

	#[test]
	fn test_pipe_operator_simple() {
		let bump = Bump::new();
		let source = "from users | sort {name}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 2);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Sort(_)));
	}

	#[test]
	fn test_pipe_operator_multiple() {
		let bump = Bump::new();
		let source = "from users | filter {age > 18} | sort {name} | take {10}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 4);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Filter(_)));
		assert!(matches!(statement[2], Ast::Sort(_)));
		assert!(matches!(statement[3], Ast::Take(_)));
	}

	#[test]
	fn test_pipe_with_system_tables() {
		let bump = Bump::new();
		let source = "from system::tables | sort {id}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 2);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Sort(_)));
	}

	#[test]
	fn test_newline_still_works() {
		let bump = Bump::new();
		let source = "from users\nsort {name}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 2);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Sort(_)));
	}

	#[test]
	fn test_output_prefix_first_statement() {
		let bump = Bump::new();
		let source = "OUTPUT FROM users; FROM orders";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 2);
		assert!(result[0].is_output, "First statement should have is_output = true");
		assert!(!result[1].is_output, "Second statement should have is_output = false");
		assert!(matches!(result[0].nodes[0], Ast::From(_)));
		assert!(matches!(result[1].nodes[0], Ast::From(_)));
	}

	#[test]
	fn test_output_prefix_not_present() {
		let bump = Bump::new();
		let source = "FROM users; FROM orders";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 2);
		assert!(!result[0].is_output);
		assert!(!result[1].is_output);
	}

	#[test]
	fn test_output_prefix_multiple() {
		let bump = Bump::new();
		let source = "OUTPUT FROM users; OUTPUT FROM products; FROM orders";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 3);
		assert!(result[0].is_output);
		assert!(result[1].is_output);
		assert!(!result[2].is_output);
	}

	#[test]
	fn test_mixed_pipe_and_newline() {
		let bump = Bump::new();
		let source = "from users | filter {age > 18}\nsort {name} | take {10}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 4);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Filter(_)));
		assert!(matches!(statement[2], Ast::Sort(_)));
		assert!(matches!(statement[3], Ast::Take(_)));
	}
}
