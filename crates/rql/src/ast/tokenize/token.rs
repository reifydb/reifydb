// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::Fragment;

pub use super::{keyword::Keyword, operator::Operator, separator::Separator};

#[derive(Debug, Clone)]
pub struct Token {
	pub kind: TokenKind,
	pub fragment: Fragment,
}

impl PartialEq for Token {
	fn eq(&self, other: &Self) -> bool {
		self.kind == other.kind && self.value() == other.value()
	}
}

impl Token {
	pub fn is_eof(&self) -> bool {
		self.kind == TokenKind::EOF
	}
	pub fn is_identifier(&self) -> bool {
		self.kind == TokenKind::Identifier
	}
	pub fn is_literal(&self, literal: Literal) -> bool {
		self.kind == TokenKind::Literal(literal)
	}
	pub fn is_separator(&self, separator: Separator) -> bool {
		self.kind == TokenKind::Separator(separator)
	}
	pub fn is_keyword(&self, keyword: Keyword) -> bool {
		self.kind == TokenKind::Keyword(keyword)
	}
	pub fn is_operator(&self, operator: Operator) -> bool {
		self.kind == TokenKind::Operator(operator)
	}
	pub fn is_variable(&self) -> bool {
		self.kind == TokenKind::Variable
	}
	pub fn value(&self) -> &str {
		self.fragment.text()
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TokenKind {
	EOF,
	Keyword(Keyword),
	Identifier,
	Literal(Literal),
	Operator(Operator),
	Variable,
	Separator(Separator),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Literal {
	False,
	Number,
	Temporal,
	Text,
	True,
	Undefined,
}
