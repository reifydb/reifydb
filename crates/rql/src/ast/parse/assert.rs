// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstAssert,
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Literal, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_assert(&mut self) -> Result<AstAssert<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(Keyword::Assert)?;

		// Check for ASSERT ERROR
		let expect_error = if !self.is_eof() && self.current()?.kind == TokenKind::Keyword(Keyword::Error) {
			self.advance()?;
			true
		} else {
			false
		};

		self.consume_operator(Operator::OpenCurly)?;

		// Determine if the body is multi-statement by checking for semicolons
		// before the matching close-curly. We peek ahead without consuming.
		let is_multi = expect_error || self.is_multi_statement_block();

		let (node, rql) = if is_multi {
			// Multi-statement or ASSERT ERROR: capture body source text
			let body_start_pos = self.position;

			// Parse and discard body nodes to advance past them
			loop {
				self.skip_new_line()?;

				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let _node = self.parse_node(Precedence::None)?;

				// Handle pipe operator
				if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
					self.advance()?;
					continue;
				}

				// Try to consume separator
				self.consume_if(TokenKind::Separator(Separator::NewLine))?;
				self.consume_if(TokenKind::Separator(Separator::Semicolon))?;
			}

			// Capture body source
			let body_end_pos = self.position;
			let rql = if body_start_pos < body_end_pos {
				let start = self.tokens[body_start_pos].fragment.offset();
				let end = self.tokens[body_end_pos - 1].fragment.source_end();
				self.source[start..end].trim().to_string()
			} else {
				String::new()
			};

			(None, Some(rql))
		} else {
			// Single-expression ASSERT (pipeline-compatible)
			let node = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
			(Some(node), None)
		};

		self.consume_operator(Operator::CloseCurly)?;

		// Optionally consume a trailing string literal message
		let message = if !self.is_eof() && self.current()?.kind == TokenKind::Literal(Literal::Text) {
			Some(self.advance()?)
		} else {
			None
		};

		Ok(AstAssert {
			token,
			node,
			body: rql,
			expect_error,
			message,
			rql: self.source_since(start),
		})
	}

	/// Peek ahead to detect if a curly-brace block contains multiple statements
	/// (semicolons or newlines separating nodes). Does not consume any tokens.
	fn is_multi_statement_block(&self) -> bool {
		let mut depth = 1u32;
		let mut pos = self.position;
		while pos < self.tokens.len() {
			match self.tokens[pos].kind {
				TokenKind::Operator(Operator::OpenCurly) => depth += 1,
				TokenKind::Operator(Operator::CloseCurly) => {
					depth -= 1;
					if depth == 0 {
						return false;
					}
				}
				TokenKind::Separator(Separator::Semicolon) if depth == 1 => return true,
				_ => {}
			}
			pos += 1;
		}
		false
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{ast::Ast, parse::parse},
		bump::Bump,
		token::{keyword::Keyword, token::TokenKind, tokenize},
	};

	#[test]
	fn test_assert_simple() {
		let bump = Bump::new();
		let source = "ASSERT { 1 + 1 == 2 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let assert_node = result[0].first_unchecked().as_assert();
		assert_eq!(assert_node.token.kind, TokenKind::Keyword(Keyword::Assert));
		assert!(assert_node.node.is_some());
		assert!(assert_node.body.is_none());
		assert!(!assert_node.expect_error);
		assert!(assert_node.message.is_none());
	}

	#[test]
	fn test_assert_with_message() {
		let bump = Bump::new();
		let source = r#"ASSERT { x > 0 } "must be positive""#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let assert_node = result[0].first_unchecked().as_assert();
		assert!(assert_node.node.is_some());
		assert!(assert_node.message.is_some());
	}

	#[test]
	fn test_assert_in_pipeline() {
		let bump = Bump::new();
		let source = "FROM users | ASSERT { count(*) > 0 } | MAP { name }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.len(), 3);
		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Assert(_)));
		assert!(matches!(statement[2], Ast::Map(_)));
	}

	#[test]
	fn test_assert_multi_statement() {
		let bump = Bump::new();
		let source = "ASSERT { LET $x = 5; $x > 3 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let assert_node = result[0].first_unchecked().as_assert();
		assert!(assert_node.node.is_none());
		assert!(assert_node.body.is_some());
		assert!(!assert_node.expect_error);
		let body = assert_node.body.as_ref().unwrap();
		assert!(body.contains("LET"));
		assert!(body.contains("$x"));
	}

	#[test]
	fn test_assert_error() {
		let bump = Bump::new();
		let source = "ASSERT ERROR { INSERT test::nonexistent [{ id: 1 }] }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let assert_node = result[0].first_unchecked().as_assert();
		assert!(assert_node.node.is_none());
		assert!(assert_node.body.is_some());
		assert!(assert_node.expect_error);
	}

	#[test]
	fn test_assert_error_with_message() {
		let bump = Bump::new();
		let source = r#"ASSERT ERROR { INSERT test::nonexistent [{ id: 1 }] } "should fail""#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let assert_node = result[0].first_unchecked().as_assert();
		assert!(assert_node.expect_error);
		assert!(assert_node.message.is_some());
	}
}
