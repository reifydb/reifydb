// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Fragment, OwnedFragment};

pub use super::{
	keyword::Keyword, operator::Operator, parameter::ParameterKind,
	separator::Separator,
};

#[derive(Debug, Clone)]
pub struct Token<'a> {
	pub kind: TokenKind,
	pub fragment: Fragment<'a>,
}

impl<'a> PartialEq for Token<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.kind == other.kind && self.value() == other.value()
	}
}

impl<'a> From<Token<'a>> for OwnedFragment {
	fn from(value: Token<'a>) -> Self {
		value.fragment.into_owned()
	}
}

impl<'a> Token<'a> {
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
	Parameter(ParameterKind),
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
