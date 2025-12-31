// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! RQL v2 Tokenizer
//!
//! Tokenizes RQL input into a stream of tokens using a bump allocator
//! for efficient memory management.

mod cursor;
mod error;
mod explain;
mod keyword;
mod lexer;
mod literal;
mod operator;
mod punctuation;
mod span;
mod token;

use bumpalo::Bump;
pub use error::LexError;
pub use explain::explain_tokenize;
pub use keyword::Keyword;
pub use lexer::{Lexer, TokenizeResult};
pub use literal::LiteralKind;
pub use operator::Operator;
pub use punctuation::Punctuation;
pub use span::{Span, Spanned};
pub use token::{Token, TokenKind};

/// Tokenize RQL input using the provided bump allocator.
///
/// # Arguments
///
/// * `source` - The RQL source code to token
/// * `bump` - The bump allocator to use for token storage
///
/// # Returns
///
/// A `TokenizeResult` containing the tokens and source, or a `LexError` if
/// tokenization fails.
pub fn tokenize<'bump>(source: &str, bump: &'bump Bump) -> Result<TokenizeResult<'bump>, LexError> {
	Lexer::new(source, bump).tokenize()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_tokenize_simple() {
		let bump = Bump::new();
		let result = tokenize("FROM users MAP { * }", &bump).unwrap();

		assert_eq!(result.len(), 7); // FROM, users, MAP, {, *, }, EOF
		assert!(matches!(result.tokens[0].kind, TokenKind::Keyword(Keyword::From)));
		assert!(matches!(result.tokens[1].kind, TokenKind::Identifier));
		assert!(matches!(result.tokens[2].kind, TokenKind::Keyword(Keyword::Map)));
		assert!(matches!(result.tokens[3].kind, TokenKind::Punctuation(Punctuation::OpenCurly)));
		assert!(matches!(result.tokens[4].kind, TokenKind::Operator(Operator::Asterisk)));
		assert!(matches!(result.tokens[5].kind, TokenKind::Punctuation(Punctuation::CloseCurly)));
		assert!(matches!(result.tokens[6].kind, TokenKind::Eof));
	}

	#[test]
	fn test_tokenize_with_whitespace() {
		let bump = Bump::new();
		let result = tokenize("   FROM   users   MAP   {  *  }   ", &bump).unwrap();

		assert_eq!(result.len(), 7);
		assert!(matches!(result.tokens[0].kind, TokenKind::Keyword(Keyword::From)));
	}

	#[test]
	fn test_tokenize_string_raw() {
		let bump = Bump::new();
		let result = tokenize(r#"'hello\nworld'"#, &bump).unwrap();

		// String content includes the raw escape sequence (no processing)
		assert_eq!(result.text(&result.tokens[0]), r#"hello\nworld"#);
	}

	#[test]
	fn test_tokenize_complex_query() {
		let bump = Bump::new();
		let query = "FROM users FILTER age > 18 AND status = 'active' MAP { name, age }";
		let result = tokenize(query, &bump).unwrap();

		assert!(matches!(result.tokens[0].kind, TokenKind::Keyword(Keyword::From)));
		assert!(matches!(result.tokens[1].kind, TokenKind::Identifier));
		assert!(matches!(result.tokens[2].kind, TokenKind::Keyword(Keyword::Filter)));
		assert!(matches!(result.tokens[3].kind, TokenKind::Identifier)); // age
		assert!(matches!(result.tokens[4].kind, TokenKind::Operator(Operator::RightAngle)));
	}

	#[test]
	fn test_tokenize_scan_keyword() {
		let bump = Bump::new();
		let result = tokenize("SCAN users | FILTER age > 21", &bump).unwrap();

		assert!(matches!(result.tokens[0].kind, TokenKind::Keyword(Keyword::Scan)));
		assert!(matches!(result.tokens[2].kind, TokenKind::Operator(Operator::Pipe)));
		assert!(matches!(result.tokens[3].kind, TokenKind::Keyword(Keyword::Filter)));
	}
}
