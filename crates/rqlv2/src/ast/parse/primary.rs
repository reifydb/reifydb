// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Primary expression parsing (literals, identifiers, collections, etc.)

use bumpalo::collections::Vec as BumpVec;

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
	pratt::Precedence,
};
use crate::{
	ast::{Expr, Statement, expr::*, stmt::ExprStmt},
	token::{Keyword, LiteralKind, Operator, Punctuation, Span, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse a unary expression.
	pub(super) fn parse_unary(&mut self, op: UnaryOp) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span;
		let operand = self.parse_expr(Precedence::Prefix)?;
		let span = start_span.merge(&operand.span());

		Ok(self.alloc(Expr::Unary(UnaryExpr::new(op, operand, span))))
	}

	/// Parse parenthesized expression or tuple.
	pub(super) fn parse_paren_or_tuple(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume (

		// Empty tuple
		if self.check_punct(Punctuation::CloseParen) {
			let end_span = self.advance().span;
			return Ok(self.alloc(Expr::Tuple(TupleExpr::new(&[], start_span.merge(&end_span)))));
		}

		// Parse first expression
		let first = self.parse_expr(Precedence::None)?;

		// Check for comma (tuple) or close paren (grouping)
		if self.try_consume_punct(Punctuation::Comma) {
			// It's a tuple
			let mut elements = BumpVec::new_in(self.bump);
			elements.push(*first);

			while !self.check_punct(Punctuation::CloseParen) {
				let elem = self.parse_expr(Precedence::None)?;
				elements.push(*elem);

				if !self.try_consume_punct(Punctuation::Comma) {
					break;
				}
			}

			let end_span = self.expect_punct(Punctuation::CloseParen)?;
			Ok(self.alloc(Expr::Tuple(TupleExpr::new(
				elements.into_bump_slice(),
				start_span.merge(&end_span),
			))))
		} else {
			// It's a parenthesized expression
			self.expect_punct(Punctuation::CloseParen)?;
			Ok(self.alloc(Expr::Paren(first)))
		}
	}

	/// Parse a list expression: [a, b, c]
	pub(super) fn parse_list(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume [

		let mut elements = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseBracket) && !self.is_eof() {
			let elem = self.parse_expr(Precedence::None)?;
			elements.push(*elem);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseBracket)?;

		Ok(self.alloc(Expr::List(ListExpr::new(elements.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse inline object or subquery: { ... }
	pub(super) fn parse_inline_or_subquery(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume {

		// Empty object
		if self.check_punct(Punctuation::CloseCurly) {
			let end_span = self.advance().span;
			return Ok(self.alloc(Expr::Inline(InlineExpr::new(&[], start_span.merge(&end_span)))));
		}

		// Check if it's a subquery (starts with FROM or other query keyword)
		if matches!(
			self.current().kind,
			TokenKind::Keyword(Keyword::From)
				| TokenKind::Keyword(Keyword::Filter)
				| TokenKind::Keyword(Keyword::Map)
				| TokenKind::Keyword(Keyword::Select)
		) {
			return self.parse_subquery(start_span);
		}

		// It's an inline object: { key: value, ... }
		let mut fields = BumpVec::new_in(self.bump);

		loop {
			// Parse key
			if !matches!(self.current().kind, TokenKind::Identifier | TokenKind::QuotedIdentifier) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}

			let key_token = self.advance();
			let key = self.token_text(&key_token);

			// Expect colon
			self.expect_operator(Operator::Colon)?;

			// Parse value
			let value = self.parse_expr(Precedence::None)?;

			fields.push(InlineField::new(key, value));

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Inline(InlineExpr::new(fields.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse a subquery: { FROM ... | ... }
	pub(super) fn parse_subquery(&mut self, start_span: Span) -> Result<&'bump Expr<'bump>, ParseError> {
		let mut stages = BumpVec::new_in(self.bump);

		// Parse first stage
		let first = self.parse_expr(Precedence::None)?;
		stages.push(*first);

		// Parse pipeline
		while self.try_consume_operator(Operator::Pipe) {
			let stage = self.parse_expr(Precedence::None)?;
			stages.push(*stage);
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::SubQuery(SubQueryExpr::new(stages.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse a literal.
	pub(super) fn parse_literal(&mut self, lit: LiteralKind) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.advance();
		let text = self.token_text(&token);
		let span = token.span;

		let literal = match lit {
			LiteralKind::Integer => Literal::integer(text, span),
			LiteralKind::Float => Literal::float(text, span),
			LiteralKind::String => {
				// Strip quotes from string
				let content = if text.len() >= 2 {
					&text[1..text.len() - 1]
				} else {
					text
				};
				Literal::string(self.alloc_str(content), span)
			}
			LiteralKind::True => Literal::bool(true, span),
			LiteralKind::False => Literal::bool(false, span),
			LiteralKind::Undefined => Literal::undefined(span),
			LiteralKind::Temporal => Literal::temporal(text, span),
		};

		Ok(self.alloc(Expr::Literal(literal)))
	}

	/// Parse an identifier.
	pub(super) fn parse_identifier(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.advance();
		let name = self.token_text(&token);
		let span = token.span;

		Ok(self.alloc(Expr::Identifier(Identifier::new(name, span))))
	}

	/// Parse a quoted identifier.
	pub(super) fn parse_quoted_identifier(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.advance();
		let text = self.token_text(&token);
		// Strip backticks
		let name = if text.len() >= 2 {
			&text[1..text.len() - 1]
		} else {
			text
		};
		let name = self.alloc_str(name);
		let span = token.span;

		Ok(self.alloc(Expr::Identifier(Identifier::new(name, span))))
	}

	/// Parse a variable.
	pub(super) fn parse_variable(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.advance();
		let text = self.token_text(&token);
		// Strip $ prefix
		let name = if text.starts_with('$') {
			&text[1..]
		} else {
			text
		};
		let name = self.alloc_str(name);
		let span = token.span;

		// Check for special $env
		if name == "env" {
			return Ok(self.alloc(Expr::Environment(EnvironmentExpr::new(span))));
		}

		Ok(self.alloc(Expr::Variable(Variable::new(name, span))))
	}

	/// Parse keyword expressions (FROM, FILTER, MAP, etc.)
	pub(super) fn parse_keyword_expr(&mut self, kw: Keyword) -> Result<&'bump Expr<'bump>, ParseError> {
		match kw {
			Keyword::From => self.parse_from(),
			Keyword::Filter => self.parse_filter(),
			Keyword::Map | Keyword::Select => self.parse_map(),
			Keyword::Extend => self.parse_extend(),
			Keyword::Sort => self.parse_sort(),
			Keyword::Take => self.parse_take(),
			Keyword::Distinct => self.parse_distinct(),
			Keyword::Aggregate => self.parse_aggregate(),
			Keyword::If => self.parse_if_expr(),
			// JOIN variants
			Keyword::Join => self.parse_join(),
			Keyword::Inner => self.parse_inner_join(),
			Keyword::Left => self.parse_left_join(),
			Keyword::Natural => self.parse_natural_join(),
			Keyword::True => {
				let span = self.advance().span;
				Ok(self.alloc(Expr::Literal(Literal::bool(true, span))))
			}
			Keyword::False => {
				let span = self.advance().span;
				Ok(self.alloc(Expr::Literal(Literal::bool(false, span))))
			}
			Keyword::Undefined => {
				let span = self.advance().span;
				Ok(self.alloc(Expr::Literal(Literal::undefined(span))))
			}
			Keyword::Rownum => {
				let span = self.advance().span;
				Ok(self.alloc(Expr::Rownum(RownumExpr::new(span))))
			}
			// Treat other keywords as identifiers (e.g., VALUE, KEY, TABLE can be column names)
			_ => {
				let token = self.advance();
				let name = self.token_text(&token);
				Ok(self.alloc(Expr::Identifier(Identifier::new(name, token.span))))
			}
		}
	}

	/// Parse wildcard: *
	pub(super) fn parse_wildcard(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let span = self.advance().span;
		Ok(self.alloc(Expr::Wildcard(WildcardExpr::new(span))))
	}

	/// Parse a function call: func(args)
	pub(super) fn parse_call(&mut self, function: &'bump Expr<'bump>) -> Result<&'bump Expr<'bump>, ParseError> {
		self.advance(); // consume (

		let mut arguments = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseParen) && !self.is_eof() {
			let arg = self.parse_expr(Precedence::None)?;
			arguments.push(*arg);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseParen)?;
		let span = function.span().merge(&end_span);

		Ok(self.alloc(Expr::Call(CallExpr::new(function, arguments.into_bump_slice(), span))))
	}

	/// Parse BETWEEN expression: x BETWEEN low AND high
	pub(super) fn parse_between(&mut self, value: &'bump Expr<'bump>) -> Result<&'bump Expr<'bump>, ParseError> {
		self.advance(); // consume BETWEEN

		let lower = self.parse_expr(Precedence::Comparison)?;

		self.expect_operator(Operator::And)?;

		let upper = self.parse_expr(Precedence::Comparison)?;

		let span = value.span().merge(&upper.span());
		Ok(self.alloc(Expr::Between(BetweenExpr::new(value, lower, upper, span))))
	}

	/// Parse IN expression: x IN [list] or x NOT IN [list]
	pub(super) fn parse_in(
		&mut self,
		value: &'bump Expr<'bump>,
		negated: bool,
	) -> Result<&'bump Expr<'bump>, ParseError> {
		if negated {
			self.advance(); // consume NOT
		}
		self.advance(); // consume IN

		let list = self.parse_expr(Precedence::Comparison)?;
		let span = value.span().merge(&list.span());

		Ok(self.alloc(Expr::In(InExpr::new(value, list, negated, span))))
	}

	/// Parse IF expression.
	pub(super) fn parse_if_expr(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume IF

		let condition = self.parse_expr(Precedence::LogicOr)?;

		// Expect THEN or block
		let then_expr = if self.try_consume_keyword(Keyword::Then) {
			self.parse_expr(Precedence::None)?
		} else {
			return Err(self.error(ParseErrorKind::ExpectedKeyword(Keyword::Then)));
		};
		let then_branch = self.wrap_expr_as_block(then_expr);

		// Parse else-if branches
		let mut else_ifs = BumpVec::new_in(self.bump);
		while self.try_consume_keyword(Keyword::Else) {
			if self.try_consume_keyword(Keyword::If) {
				let cond = self.parse_expr(Precedence::LogicOr)?;
				self.expect_keyword(Keyword::Then)?;
				let branch_expr = self.parse_expr(Precedence::None)?;
				let branch = self.wrap_expr_as_block(branch_expr);
				else_ifs.push(ElseIf::new(cond, branch, cond.span().merge(&branch_expr.span())));
			} else {
				// Final else
				let else_expr = self.parse_expr(Precedence::None)?;
				let else_branch = self.wrap_expr_as_block(else_expr);
				let span = start_span.merge(&else_expr.span());
				return Ok(self.alloc(Expr::IfExpr(IfExpr::new(
					condition,
					then_branch,
					else_ifs.into_bump_slice(),
					Some(else_branch),
					span,
				))));
			}
		}

		let end_span = if let Some(last) = else_ifs.last() {
			last.span
		} else {
			then_expr.span()
		};

		Ok(self.alloc(Expr::IfExpr(IfExpr::new(
			condition,
			then_branch,
			else_ifs.into_bump_slice(),
			None,
			start_span.merge(&end_span),
		))))
	}

	/// Wrap a single expression as a statement block.
	fn wrap_expr_as_block(&self, expr: &'bump Expr<'bump>) -> &'bump [Statement<'bump>] {
		let stmt = Statement::Expression(ExprStmt::new(expr, expr.span()));
		self.bump.alloc_slice_copy(&[stmt])
	}
}
