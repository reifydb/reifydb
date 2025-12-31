// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! JOIN expression parsing (JOIN, LEFT JOIN, INNER JOIN, NATURAL JOIN).
//!
//! Syntax (subquery - executes a pipeline):
//! - JOIN { subquery } AS alias USING (left_col, right_col) [AND|OR (left_col, right_col)]...
//! - LEFT JOIN { subquery } AS alias USING (left_col, right_col) [AND|OR (left_col, right_col)]...
//!
//! Syntax (primitive - direct table reference):
//! - JOIN table AS alias USING (left_col, right_col) [AND|OR (left_col, right_col)]...
//! - LEFT JOIN namespace.table AS alias USING (left_col, right_col) [AND|OR (left_col, right_col)]...
//!
//! Natural joins:
//! - NATURAL JOIN { subquery } AS alias
//! - NATURAL JOIN table AS alias

use bumpalo::collections::Vec as BumpVec;

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
	pratt::Precedence,
};
use crate::{
	ast::{Expr, expr::*},
	token::{Keyword, Operator, Punctuation, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse JOIN expression (implicit INNER JOIN).
	/// Syntax: JOIN { subquery } AS alias USING (...) or JOIN table AS alias USING (...)
	pub(super) fn parse_join(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume JOIN

		let source = self.parse_join_source()?;

		// Required: AS alias
		self.expect_operator(Operator::As)?;
		let alias = self.parse_join_alias()?;

		// Required: USING clause
		let using_clause = self.parse_using_clause()?;

		let span = start_span.merge(&using_clause.span);

		Ok(self.alloc(Expr::Join(JoinExpr::Inner(JoinInner {
			source,
			using_clause,
			alias,
			span,
		}))))
	}

	/// Parse INNER JOIN expression.
	/// Syntax: INNER JOIN { subquery } AS alias USING (...) or INNER JOIN table AS alias USING (...)
	pub(super) fn parse_inner_join(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume INNER
		self.expect_keyword(Keyword::Join)?;

		let source = self.parse_join_source()?;

		// Required: AS alias
		self.expect_operator(Operator::As)?;
		let alias = self.parse_join_alias()?;

		// Required: USING clause
		let using_clause = self.parse_using_clause()?;

		let span = start_span.merge(&using_clause.span);

		Ok(self.alloc(Expr::Join(JoinExpr::Inner(JoinInner {
			source,
			using_clause,
			alias,
			span,
		}))))
	}

	/// Parse LEFT JOIN expression.
	/// Syntax: LEFT JOIN { subquery } AS alias USING (...) or LEFT JOIN table AS alias USING (...)
	pub(super) fn parse_left_join(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume LEFT
		self.expect_keyword(Keyword::Join)?;

		let source = self.parse_join_source()?;

		// Required: AS alias
		self.expect_operator(Operator::As)?;
		let alias = self.parse_join_alias()?;

		// Required: USING clause
		let using_clause = self.parse_using_clause()?;

		let span = start_span.merge(&using_clause.span);

		Ok(self.alloc(Expr::Join(JoinExpr::Left(JoinLeft {
			source,
			using_clause,
			alias,
			span,
		}))))
	}

	/// Parse NATURAL JOIN expression.
	/// Syntax: NATURAL [LEFT|INNER] JOIN { subquery } AS alias or NATURAL JOIN table AS alias
	pub(super) fn parse_natural_join(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume NATURAL

		// Check for optional LEFT or INNER modifier
		if self.try_consume_keyword(Keyword::Left) {
			// NATURAL LEFT JOIN
		} else if self.try_consume_keyword(Keyword::Inner) {
			// NATURAL INNER JOIN
		}

		self.expect_keyword(Keyword::Join)?;

		let source = self.parse_join_source()?;

		// Required: AS alias
		self.expect_operator(Operator::As)?;
		let alias_token = self.advance();
		if !matches!(alias_token.kind, TokenKind::Identifier | TokenKind::QuotedIdentifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let alias = self.token_text(&alias_token);
		let end_span = alias_token.span;

		let span = start_span.merge(&end_span);

		Ok(self.alloc(Expr::Join(JoinExpr::Natural(JoinNatural {
			source,
			alias,
			span,
		}))))
	}

	/// Parse the source of a JOIN - either a subquery { ... } or direct table reference.
	fn parse_join_source(&mut self) -> Result<JoinSource<'bump>, ParseError> {
		if self.check_punct(Punctuation::OpenCurly) {
			// Subquery: { FROM ... | ... }
			let start_span = self.advance().span; // consume {

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

			let subquery = self.alloc(Expr::SubQuery(SubQueryExpr::new(
				stages.into_bump_slice(),
				start_span.merge(&end_span),
			)));
			Ok(JoinSource::SubQuery(subquery))
		} else {
			// Direct table reference: table, namespace.table, or ns1::ns2.table
			let qualified = self.parse_qualified_name()?;

			let source = SourceRef::new(qualified.name, qualified.span).with_namespace(qualified.namespace);

			Ok(JoinSource::Primitive(JoinPrimitive {
				source,
			}))
		}
	}

	/// Parse the alias identifier after AS.
	fn parse_join_alias(&mut self) -> Result<&'bump str, ParseError> {
		if !matches!(self.current().kind, TokenKind::Identifier | TokenKind::QuotedIdentifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let token = self.advance();
		Ok(self.token_text(&token))
	}

	/// Parse USING clause: USING (left, right) [AND|OR (left, right)]...
	fn parse_using_clause(&mut self) -> Result<UsingClause<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Using)?;
		let mut pairs = BumpVec::new_in(self.bump);
		let mut end_span;

		loop {
			// Expect: (expression, expression)
			self.expect_punct(Punctuation::OpenParen)?;
			let left = self.parse_expr(Precedence::None)?;

			// Expect comma
			self.expect_punct(Punctuation::Comma)?;

			let right = self.parse_expr(Precedence::None)?;
			end_span = self.expect_punct(Punctuation::CloseParen)?;

			// Check for connector (AND or OR)
			let connector = if self.check_operator(Operator::And) {
				self.advance();
				Some(JoinConnector::And)
			} else if self.check_operator(Operator::Or) {
				self.advance();
				Some(JoinConnector::Or)
			} else {
				None
			};

			let has_more = connector.is_some();
			pairs.push(JoinPair::new(left, right, connector));

			if !has_more {
				break;
			}
		}

		Ok(UsingClause::new(pairs.into_bump_slice(), start_span.merge(&end_span)))
	}
}
