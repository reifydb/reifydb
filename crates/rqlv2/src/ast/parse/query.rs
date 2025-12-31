// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Query expression parsing (FROM, FILTER, MAP, etc.)

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
	/// Parse FROM expression.
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
	pub(super) fn parse_filter(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume FILTER

		let predicate = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&predicate.span());

		Ok(self.alloc(Expr::Filter(FilterExpr::new(predicate, span))))
	}

	/// Parse MAP/SELECT expression.
	pub(super) fn parse_map(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume MAP or SELECT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut projections = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let proj = self.parse_expr(Precedence::None)?; // Allow AS binding
			projections.push(*proj);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Map(MapExpr::new(projections.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse EXTEND expression.
	pub(super) fn parse_extend(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume EXTEND

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut extensions = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let ext = self.parse_expr(Precedence::None)?;
			extensions.push(*ext);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Extend(ExtendExpr::new(extensions.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse SORT expression.
	/// Syntax: SORT { key: ASC, key2: DESC } or SORT { key } (defaults to ASC)
	pub(super) fn parse_sort(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume SORT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let expr = self.parse_expr(Precedence::Comparison)?;

			// Check for colon followed by direction, or just use default
			let direction = if self.try_consume_operator(Operator::Colon) {
				if self.try_consume_keyword(Keyword::Asc) {
					Some(SortDirection::Asc)
				} else if self.try_consume_keyword(Keyword::Desc) {
					Some(SortDirection::Desc)
				} else {
					return Err(self.error(ParseErrorKind::ExpectedKeyword(Keyword::Asc)));
				}
			} else {
				None // Default direction
			};

			columns.push(SortColumn::new(expr, direction));

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Sort(SortExpr::new(columns.into_bump_slice(), start_span.merge(&end_span)))))
	}

	/// Parse TAKE expression.
	pub(super) fn parse_take(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume TAKE

		let count = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&count.span());

		Ok(self.alloc(Expr::Take(TakeExpr::new(count, span))))
	}

	/// Parse DISTINCT expression.
	pub(super) fn parse_distinct(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume DISTINCT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		// Parse columns (can be empty for DISTINCT {} meaning all columns)
		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let col = self.parse_expr(Precedence::Comparison)?;
			columns.push(*col);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Distinct(DistinctExpr::new(
			columns.into_bump_slice(),
			start_span.merge(&end_span),
		))))
	}

	/// Parse AGGREGATE expression: AGGREGATE { expr, ... } BY { col, ... }
	pub(super) fn parse_aggregate(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume AGGREGATE

		// Require opening brace for aggregations
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut aggregations = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let agg = self.parse_expr(Precedence::None)?;
			aggregations.push(*agg);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		// Require BY keyword
		self.expect_keyword(Keyword::By)?;

		// Require opening brace for group-by columns
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut group_by = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let col = self.parse_expr(Precedence::Comparison)?;
			group_by.push(*col);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Aggregate(AggregateExpr::new(
			group_by.into_bump_slice(),
			aggregations.into_bump_slice(),
			start_span.merge(&end_span),
		))))
	}
}
