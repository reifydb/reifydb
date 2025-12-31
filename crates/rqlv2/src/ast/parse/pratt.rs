// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pratt parser implementation with precedence climbing.

use bumpalo::collections::Vec as BumpVec;

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
};
use crate::{
	ast::{Expr, expr::*},
	token::{Keyword, LiteralKind, Operator, Punctuation, Span, Token, TokenKind},
};

/// Operator precedence levels (higher = binds tighter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precedence {
	None = 0,
	Assignment = 1, // :=, as, :
	LogicOr = 2,    // OR, XOR, ||
	LogicAnd = 3,   // AND, &&
	Comparison = 4, // =, !=, <, <=, >, >=, IN, BETWEEN
	Term = 5,       // +, -
	Factor = 6,     // *, /, %
	Prefix = 7,     // -, NOT, !
	Call = 8,       // ()
	Primary = 9,    // ., ::, ->
}

impl Precedence {
	/// Get precedence for a token in infix position.
	pub fn for_token(token: &Token) -> Self {
		match token.kind {
			TokenKind::Operator(op) => match op {
				Operator::ColonEqual => Precedence::Assignment,
				Operator::As => Precedence::Assignment,
				Operator::Colon => Precedence::Assignment,

				Operator::Or | Operator::DoublePipe | Operator::Xor => Precedence::LogicOr,
				Operator::And | Operator::DoubleAmpersand => Precedence::LogicAnd,

				Operator::Equal
				| Operator::DoubleEqual
				| Operator::BangEqual
				| Operator::LeftAngle
				| Operator::LeftAngleEqual
				| Operator::RightAngle
				| Operator::RightAngleEqual => Precedence::Comparison,

				Operator::Plus | Operator::Minus => Precedence::Term,
				Operator::Asterisk | Operator::Slash | Operator::Percent => Precedence::Factor,

				Operator::Dot | Operator::DoubleColon | Operator::Arrow => Precedence::Primary,

				_ => Precedence::None,
			},
			TokenKind::Punctuation(Punctuation::OpenParen) => Precedence::Call,
			TokenKind::Keyword(Keyword::In) => Precedence::Comparison,
			TokenKind::Keyword(Keyword::Between) => Precedence::Comparison,
			_ => Precedence::None,
		}
	}
}

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse an expression with the given minimum precedence.
	pub fn parse_expr(&mut self, min_precedence: Precedence) -> Result<&'bump Expr<'bump>, ParseError> {
		// Parse prefix (primary) expression
		let mut left = self.parse_prefix()?;

		// Parse infix expressions while precedence allows
		while !self.is_eof() {
			let current = self.current();
			let precedence = Precedence::for_token(current);

			if precedence <= min_precedence {
				break;
			}

			left = self.parse_infix(left, precedence)?;
		}

		Ok(left)
	}

	/// Parse a prefix (primary) expression.
	fn parse_prefix(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.current();

		match token.kind {
			// Unary operators
			TokenKind::Operator(Operator::Minus) => self.parse_unary(UnaryOp::Neg),
			TokenKind::Operator(Operator::Plus) => self.parse_unary(UnaryOp::Plus),
			TokenKind::Operator(Operator::Bang) | TokenKind::Operator(Operator::Not) => {
				self.parse_unary(UnaryOp::Not)
			}

			// Grouping: (expr) or tuple
			TokenKind::Punctuation(Punctuation::OpenParen) => self.parse_paren_or_tuple(),

			// Collections
			TokenKind::Punctuation(Punctuation::OpenBracket) => self.parse_list(),
			TokenKind::Punctuation(Punctuation::OpenCurly) => self.parse_inline_or_subquery(),

			// Literals
			TokenKind::Literal(lit) => self.parse_literal(lit),

			// Identifiers
			TokenKind::Identifier => self.parse_identifier(),
			TokenKind::QuotedIdentifier => self.parse_quoted_identifier(),
			TokenKind::Variable => self.parse_variable(),

			// Keywords that start expressions
			TokenKind::Keyword(kw) => self.parse_keyword_expr(kw),

			// Wildcard
			TokenKind::Operator(Operator::Asterisk) => self.parse_wildcard(),

			_ => Err(self.error(ParseErrorKind::ExpectedExpression)),
		}
	}

	/// Parse a unary expression.
	fn parse_unary(&mut self, op: UnaryOp) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span;
		let operand = self.parse_expr(Precedence::Prefix)?;
		let span = start_span.merge(&operand.span());

		Ok(self.alloc(Expr::Unary(UnaryExpr::new(op, operand, span))))
	}

	/// Parse parenthesized expression or tuple.
	fn parse_paren_or_tuple(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_list(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_inline_or_subquery(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_subquery(&mut self, start_span: Span) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_literal(&mut self, lit: LiteralKind) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_identifier(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.advance();
		let name = self.token_text(&token);
		let span = token.span;

		Ok(self.alloc(Expr::Identifier(Identifier::new(name, span))))
	}

	/// Parse a quoted identifier.
	fn parse_quoted_identifier(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_variable(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_keyword_expr(&mut self, kw: Keyword) -> Result<&'bump Expr<'bump>, ParseError> {
		match kw {
			Keyword::From => self.parse_from(),
			Keyword::Filter => self.parse_filter(),
			Keyword::Map | Keyword::Select => self.parse_map(),
			Keyword::Extend => self.parse_extend(),
			Keyword::Sort => self.parse_sort(),
			Keyword::Take => self.parse_take(),
			Keyword::Distinct => self.parse_distinct(),
			Keyword::If => self.parse_if_expr(),
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
			_ => Err(self.error(ParseErrorKind::UnexpectedToken)),
		}
	}

	/// Parse wildcard: *
	fn parse_wildcard(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let span = self.advance().span;
		Ok(self.alloc(Expr::Wildcard(WildcardExpr::new(span))))
	}

	/// Parse an infix expression.
	fn parse_infix(
		&mut self,
		left: &'bump Expr<'bump>,
		precedence: Precedence,
	) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.current();

		match token.kind {
			// Function call
			TokenKind::Punctuation(Punctuation::OpenParen) => self.parse_call(left),

			// BETWEEN
			TokenKind::Keyword(Keyword::Between) => self.parse_between(left),

			// IN
			TokenKind::Keyword(Keyword::In) => self.parse_in(left, false),

			// NOT IN - must come before general operator match
			TokenKind::Operator(Operator::Not) if self.peek().kind == TokenKind::Keyword(Keyword::In) => {
				self.parse_in(left, true)
			}

			// Binary operators
			TokenKind::Operator(op) => {
				let binary_op = self.token_to_binary_op(op)?;
				self.advance();
				let right = self.parse_expr(precedence)?;
				let span = left.span().merge(&right.span());
				Ok(self.alloc(Expr::Binary(BinaryExpr::new(binary_op, left, right, span))))
			}

			_ => Err(self.error(ParseErrorKind::UnexpectedToken)),
		}
	}

	/// Convert operator token to BinaryOp.
	fn token_to_binary_op(&self, op: Operator) -> Result<BinaryOp, ParseError> {
		Ok(match op {
			Operator::Plus => BinaryOp::Add,
			Operator::Minus => BinaryOp::Sub,
			Operator::Asterisk => BinaryOp::Mul,
			Operator::Slash => BinaryOp::Div,
			Operator::Percent => BinaryOp::Rem,
			Operator::Equal | Operator::DoubleEqual => BinaryOp::Eq,
			Operator::BangEqual => BinaryOp::Ne,
			Operator::LeftAngle => BinaryOp::Lt,
			Operator::LeftAngleEqual => BinaryOp::Le,
			Operator::RightAngle => BinaryOp::Gt,
			Operator::RightAngleEqual => BinaryOp::Ge,
			Operator::And | Operator::DoubleAmpersand => BinaryOp::And,
			Operator::Or | Operator::DoublePipe => BinaryOp::Or,
			Operator::Xor => BinaryOp::Xor,
			Operator::Dot => BinaryOp::Dot,
			Operator::DoubleColon => BinaryOp::DoubleColon,
			Operator::Arrow => BinaryOp::Arrow,
			Operator::As => BinaryOp::As,
			Operator::ColonEqual => BinaryOp::Assign,
			Operator::Colon => BinaryOp::TypeAscription,
			_ => return Err(self.error(ParseErrorKind::UnexpectedToken)),
		})
	}

	/// Parse a function call: func(args)
	fn parse_call(&mut self, function: &'bump Expr<'bump>) -> Result<&'bump Expr<'bump>, ParseError> {
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
	fn parse_between(&mut self, value: &'bump Expr<'bump>) -> Result<&'bump Expr<'bump>, ParseError> {
		self.advance(); // consume BETWEEN

		let lower = self.parse_expr(Precedence::Comparison)?;

		self.expect_operator(Operator::And)?;

		let upper = self.parse_expr(Precedence::Comparison)?;

		let span = value.span().merge(&upper.span());
		Ok(self.alloc(Expr::Between(BetweenExpr::new(value, lower, upper, span))))
	}

	/// Parse IN expression: x IN [list] or x NOT IN [list]
	fn parse_in(&mut self, value: &'bump Expr<'bump>, negated: bool) -> Result<&'bump Expr<'bump>, ParseError> {
		if negated {
			self.advance(); // consume NOT
		}
		self.advance(); // consume IN

		let list = self.parse_expr(Precedence::Comparison)?;
		let span = value.span().merge(&list.span());

		Ok(self.alloc(Expr::In(InExpr::new(value, list, negated, span))))
	}

	/// Parse FROM expression.
	fn parse_from(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
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

		// Regular table reference
		if !matches!(self.current().kind, TokenKind::Identifier | TokenKind::QuotedIdentifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}

		let name_token = self.advance();
		let name = self.token_text(&name_token);
		let mut end_span = name_token.span;

		// Check for namespace qualification
		if self.check_operator(Operator::Dot) {
			self.advance();
			if !matches!(self.current().kind, TokenKind::Identifier) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let table_token = self.advance();
			let table_name = self.token_text(&table_token);
			end_span = table_token.span;

			return Ok(self.alloc(Expr::From(FromExpr::Source(
				SourceRef::new(table_name, start_span.merge(&end_span)).with_namespace(name),
			))));
		}

		Ok(self.alloc(Expr::From(FromExpr::Source(SourceRef::new(name, start_span.merge(&end_span))))))
	}

	/// Parse FILTER expression.
	fn parse_filter(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume FILTER

		let predicate = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&predicate.span());

		Ok(self.alloc(Expr::Filter(FilterExpr::new(predicate, span))))
	}

	/// Parse MAP/SELECT expression.
	fn parse_map(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume MAP or SELECT

		let mut projections = BumpVec::new_in(self.bump);

		// Optionally consume opening bracket
		let has_bracket = self.try_consume_punct(Punctuation::OpenBracket);

		loop {
			let proj = self.parse_expr(Precedence::Assignment)?; // Allow AS binding
			projections.push(*proj);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = if has_bracket {
			self.expect_punct(Punctuation::CloseBracket)?
		} else if let Some(last) = projections.last() {
			last.span()
		} else {
			start_span
		};

		Ok(self.alloc(Expr::Map(MapExpr::new(projections.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse EXTEND expression.
	fn parse_extend(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume EXTEND

		let mut extensions = BumpVec::new_in(self.bump);

		// Optionally consume opening brace
		let has_brace = self.try_consume_punct(Punctuation::OpenCurly);

		loop {
			let ext = self.parse_expr(Precedence::Assignment)?;
			extensions.push(*ext);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = if has_brace {
			self.expect_punct(Punctuation::CloseCurly)?
		} else if let Some(last) = extensions.last() {
			last.span()
		} else {
			start_span
		};

		Ok(self.alloc(Expr::Extend(ExtendExpr::new(extensions.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse SORT expression.
	fn parse_sort(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume SORT

		let mut columns = BumpVec::new_in(self.bump);

		loop {
			let expr = self.parse_expr(Precedence::Comparison)?;

			// Check for direction
			let direction = if self.try_consume_keyword(Keyword::Asc) {
				Some(SortDirection::Asc)
			} else if self.try_consume_keyword(Keyword::Desc) {
				Some(SortDirection::Desc)
			} else {
				None
			};

			columns.push(SortColumn::new(expr, direction));

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = columns.last().map(|c| c.expr.span()).unwrap_or(start_span);

		Ok(self.alloc(Expr::Sort(SortExpr::new(columns.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse TAKE expression.
	fn parse_take(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume TAKE

		let count = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&count.span());

		Ok(self.alloc(Expr::Take(TakeExpr::new(count, span))))
	}

	/// Parse DISTINCT expression.
	fn parse_distinct(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume DISTINCT

		let mut columns = BumpVec::new_in(self.bump);

		// Optional columns list
		if !self.is_at_statement_end() && !self.check_operator(Operator::Pipe) {
			loop {
				let col = self.parse_expr(Precedence::Comparison)?;
				columns.push(*col);

				if !self.try_consume_punct(Punctuation::Comma) {
					break;
				}
			}
		}

		let end_span = columns.last().map(|c| c.span()).unwrap_or(start_span);

		Ok(self.alloc(Expr::Distinct(DistinctExpr::new(
			columns.into_bump_slice(),
			start_span.merge(&end_span),
		))))
	}

	/// Parse IF expression.
	fn parse_if_expr(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume IF

		let condition = self.parse_expr(Precedence::LogicOr)?;

		// Expect THEN or block
		let then_branch = if self.try_consume_keyword(Keyword::Then) {
			self.parse_expr(Precedence::None)?
		} else {
			return Err(self.error(ParseErrorKind::ExpectedKeyword(Keyword::Then)));
		};

		// Parse else-if branches
		let mut else_ifs = BumpVec::new_in(self.bump);
		while self.try_consume_keyword(Keyword::Else) {
			if self.try_consume_keyword(Keyword::If) {
				let cond = self.parse_expr(Precedence::LogicOr)?;
				self.expect_keyword(Keyword::Then)?;
				let branch = self.parse_expr(Precedence::None)?;
				else_ifs.push(ElseIf::new(cond, branch, cond.span().merge(&branch.span())));
			} else {
				// Final else
				let else_branch = self.parse_expr(Precedence::None)?;
				let span = start_span.merge(&else_branch.span());
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
			then_branch.span()
		};

		Ok(self.alloc(Expr::IfExpr(IfExpr::new(
			condition,
			then_branch,
			else_ifs.into_bump_slice(),
			None,
			start_span.merge(&end_span),
		))))
	}
}
