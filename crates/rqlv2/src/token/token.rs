// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Token definitions.

use super::{keyword::Keyword, literal::LiteralKind, operator::Operator, punctuation::Punctuation, span::Span};

/// A token with no lifetime parameters.
///
/// Token text is accessed via `&source[span.start..span.end]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Token {
	pub kind: TokenKind,
	pub span: Span,
}

impl Token {
	/// Create a new token.
	#[inline]
	pub const fn new(kind: TokenKind, span: Span) -> Self {
		Self {
			kind,
			span,
		}
	}

	/// Get the raw text of this token from the source.
	#[inline]
	pub fn text<'a>(&self, source: &'a str) -> &'a str {
		self.span.text(source)
	}

	/// Check if this is a specific keyword.
	#[inline]
	pub fn is_keyword(&self, kw: Keyword) -> bool {
		self.kind == TokenKind::Keyword(kw)
	}

	/// Check if this is a specific operator.
	#[inline]
	pub fn is_operator(&self, op: Operator) -> bool {
		self.kind == TokenKind::Operator(op)
	}

	/// Check if this is a specific punctuation.
	#[inline]
	pub fn is_punctuation(&self, p: Punctuation) -> bool {
		self.kind == TokenKind::Punctuation(p)
	}

	/// Check if this is EOF.
	#[inline]
	pub fn is_eof(&self) -> bool {
		self.kind == TokenKind::Eof
	}
}

/// Token kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenKind {
	/// End of input.
	Eof,

	/// Identifier (raw text from span).
	Identifier,

	/// Quoted identifier (content via span, excludes backticks).
	QuotedIdentifier,

	/// Variable: $name or $123 (raw text via span, includes $).
	Variable,

	/// Keyword.
	Keyword(Keyword),

	/// Operator (symbolic and word operators like AND, OR).
	Operator(Operator),

	/// Punctuation (delimiters, separators).
	Punctuation(Punctuation),

	/// Literals.
	Literal(LiteralKind),
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_token_new() {
		let span = Span::new(0, 6, 1, 1);
		let token = Token::new(TokenKind::Keyword(Keyword::Select), span);
		assert_eq!(token.kind, TokenKind::Keyword(Keyword::Select));
		assert_eq!(token.span, span);
	}

	#[test]
	fn test_token_text() {
		let source = "SELECT * FROM users";
		let span = Span::new(0, 6, 1, 1);
		let token = Token::new(TokenKind::Keyword(Keyword::Select), span);
		assert_eq!(token.text(source), "SELECT");
	}

	#[test]
	fn test_token_is_keyword() {
		let span = Span::new(0, 6, 1, 1);
		let token = Token::new(TokenKind::Keyword(Keyword::Select), span);
		assert!(token.is_keyword(Keyword::Select));
		assert!(!token.is_keyword(Keyword::From));
	}

	#[test]
	fn test_token_is_operator() {
		let span = Span::new(0, 1, 1, 1);
		let token = Token::new(TokenKind::Operator(Operator::Plus), span);
		assert!(token.is_operator(Operator::Plus));
		assert!(!token.is_operator(Operator::Minus));
	}

	#[test]
	fn test_token_is_eof() {
		let span = Span::new(10, 10, 1, 11);
		let token = Token::new(TokenKind::Eof, span);
		assert!(token.is_eof());
	}
}
