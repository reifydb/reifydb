// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Namespace parsing utilities.
//!
//! Supports the following namespace syntax:
//! - `table` → namespace: "default", name: "table"
//! - `ns.table` → namespace: "ns", name: "table"
//! - `ns1::ns2.table` → namespace: "ns1::ns2", name: "table"
//! - `a::b::c.table` → namespace: "a::b::c", name: "table"

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
};
use crate::token::{operator::Operator, span::Span, token::TokenKind};

/// Result of parsing a qualified name (namespace + table/object name).
pub struct QualifiedName<'bump> {
	/// The namespace path (e.g., "default", "trading", "trading::analytics").
	pub namespace: &'bump str,
	/// The object name (table, view, etc.).
	pub name: &'bump str,
	/// The span covering the entire qualified name.
	pub span: Span,
}

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse a qualified name: `ident (:: ident)* [. ident]`
	///
	/// Returns a `QualifiedName` with:
	/// - `namespace`: The namespace path joined with `::`, or `"default"` if unqualified
	/// - `name`: The final object name
	///
	/// Examples:
	/// - `users` → namespace: "default", name: "users"
	/// - `trading.prices` → namespace: "trading", name: "prices"
	/// - `trading::analytics.prices` → namespace: "trading::analytics", name: "prices"
	pub fn parse_qualified_name(&mut self) -> Result<QualifiedName<'bump>, ParseError> {
		if !matches!(
			self.current().kind,
			TokenKind::Identifier | TokenKind::QuotedIdentifier | TokenKind::Keyword(_)
		) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}

		// Collect all parts separated by ::
		let mut namespace_parts: Vec<&str> = Vec::new();

		let first_token = self.advance();
		let start_span = first_token.span;
		let mut end_span = first_token.span;
		let first_name = self.token_text(&first_token);
		namespace_parts.push(first_name);

		// Consume all :: separated parts
		while self.check_operator(Operator::DoubleColon) {
			self.advance(); // consume ::
			if !matches!(
				self.current().kind,
				TokenKind::Identifier | TokenKind::QuotedIdentifier | TokenKind::Keyword(_)
			) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let part_token = self.advance();
			end_span = part_token.span;
			let part_name = self.token_text(&part_token);
			namespace_parts.push(part_name);
		}

		// Check for . separator (namespace.table)
		if self.check_operator(Operator::Dot) {
			self.advance(); // consume .
			if !matches!(
				self.current().kind,
				TokenKind::Identifier | TokenKind::QuotedIdentifier | TokenKind::Keyword(_)
			) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let name_token = self.advance();
			end_span = name_token.span;
			let name = self.token_text(&name_token);

			// Join namespace parts with ::
			let namespace = namespace_parts.join("::");
			let namespace = self.bump.alloc_str(&namespace);

			return Ok(QualifiedName {
				namespace,
				name,
				span: start_span.merge(&end_span),
			});
		}

		// No dot - single identifier is the object name, use default namespace
		// But if we have multiple parts from ::, that's an error (must end with .name)
		if namespace_parts.len() > 1 {
			return Err(self.error(ParseErrorKind::ExpectedOperator(Operator::Dot)));
		}

		let name = namespace_parts[0];
		Ok(QualifiedName {
			namespace: "default",
			name,
			span: start_span.merge(&end_span),
		})
	}
}
