// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_rql::bump::BumpFragment;

#[derive(Debug, Clone, Copy)]
pub struct Token<'bump> {
	pub kind: TokenKind,
	pub fragment: BumpFragment<'bump>,
}

impl PartialEq for Token<'_> {
	fn eq(&self, other: &Self) -> bool {
		self.kind == other.kind && self.value() == other.value()
	}
}

impl Eq for Token<'_> {}

impl<'bump> Token<'bump> {
	pub fn is_eof(&self) -> bool {
		self.kind == TokenKind::EOF
	}

	pub fn is_name(&self) -> bool {
		self.kind == TokenKind::Name
	}

	pub fn is_literal(&self) -> bool {
		matches!(
			self.kind,
			TokenKind::StringLiteral
				| TokenKind::IntLiteral | TokenKind::FloatLiteral
				| TokenKind::BooleanLiteral
		)
	}

	pub fn value(&self) -> &str {
		self.fragment.text()
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TokenKind {
	EOF,
	Name, // Identifiers, keywords
	IntLiteral,
	FloatLiteral,
	StringLiteral,
	BooleanLiteral,

	// Punctuation
	Exclamation,  // !
	Dollar,       // $
	ParenOpen,    // (
	ParenClose,   // )
	Spread,       // ...
	Colon,        // :
	Equals,       // =
	At,           // @
	BracketOpen,  // [
	BracketClose, // ]
	BraceOpen,    // {
	Pipe,         // |
	BraceClose,   // }
}
