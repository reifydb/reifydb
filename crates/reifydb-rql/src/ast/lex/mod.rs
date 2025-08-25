// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use keyword::Keyword;
use nom_locate::LocatedSpan;
pub use operator::Operator;
pub use parameter::ParameterKind;
use reifydb_core::{OwnedFragment, StatementColumn, StatementLine};
pub use separator::Separator;

mod display;
mod identifier;
mod keyword;
mod literal;
mod operator;
mod parameter;
mod separator;

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
	pub kind: TokenKind,
	pub fragment: OwnedFragment,
}

// Helper function for nom parsers - will be removed when we fully migrate
pub(crate) fn as_fragment(value: LocatedSpan<&str>) -> OwnedFragment {
	OwnedFragment::Statement {
		column: StatementColumn(value.get_column() as u32),
		line: StatementLine(value.location_line()),
		text: value.fragment().to_string(),
	}
}

impl From<Token> for OwnedFragment {
	fn from(value: Token) -> Self {
		value.fragment
	}
}

impl Token {
	pub fn is_eof(&self) -> bool {
		self.kind == EOF
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
		self.fragment.fragment()
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

use TokenKind::EOF;

use crate::ast::tokenize::tokenize;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Literal {
	False,
	Number,
	Temporal,
	Text,
	True,
	Undefined,
}

pub fn lex(input: &str) -> crate::Result<Vec<Token>> {
	tokenize(input)
}

#[cfg(test)]
mod tests {
	use TokenKind::Literal;

	use super::*;
	use crate::ast::lex::Literal::{Number, Text};

	#[test]
	fn test_keyword() {
		let tokens = lex("MAP").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(tokens[0].fragment.fragment(), "MAP");
	}

	#[test]
	fn test_identifier() {
		let tokens = lex("my_var123").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.fragment(), "my_var123");
	}

	#[test]
	fn test_number() {
		let tokens = lex("42").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "42");
	}

	#[test]
	fn test_number_negative() {
		let tokens = lex("-42").unwrap();
		assert_eq!(tokens.len(), 2);
		assert_eq!(
			tokens[0].kind,
			TokenKind::Operator(Operator::Minus)
		);
		assert_eq!(tokens[0].fragment.fragment(), "-");
		assert_eq!(tokens[1].kind, Literal(Number));
		assert_eq!(tokens[1].fragment.fragment(), "42");
	}

	#[test]
	fn test_text() {
		let tokens = lex("'hello world'").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, Literal(Text));
		assert_eq!(tokens[0].fragment.fragment(), "hello world");
	}

	#[test]
	fn test_operator() {
		let tokens = lex("+").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Operator(Operator::Plus));
		assert_eq!(tokens[0].fragment.fragment(), "+");
	}

	#[test]
	fn test_separator() {
		let tokens = lex(",").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(
			tokens[0].kind,
			TokenKind::Separator(Separator::Comma)
		);
		assert_eq!(tokens[0].fragment.fragment(), ",");
	}

	#[test]
	fn test_skips_whitespace() {
		let tokens = lex("   MAP").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(tokens[0].fragment.fragment(), "MAP");
	}

	#[test]
	fn test_desc() {
		let tokens = lex("DESC").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Desc));
		assert_eq!(tokens[0].fragment.fragment(), "DESC");
	}

	#[test]
	fn test_a() {
		let tokens = lex("a").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.fragment(), "a");
	}

	#[test]
	fn test_parameter_positional() {
		let tokens = lex("$1").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(
			tokens[0].kind,
			TokenKind::Parameter(ParameterKind::Positional(1))
		);
		assert_eq!(tokens[0].fragment.fragment(), "$1");
	}

	#[test]
	fn test_parameter_named() {
		let tokens = lex("$user_id").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(
			tokens[0].kind,
			TokenKind::Parameter(ParameterKind::Named)
		);
		assert_eq!(tokens[0].fragment.fragment(), "$user_id");
	}

	#[test]
	fn test_parameter_in_expression() {
		let tokens = lex("$1 + $2").unwrap();
		assert_eq!(tokens.len(), 3);
		assert_eq!(
			tokens[0].kind,
			TokenKind::Parameter(ParameterKind::Positional(1))
		);
		assert_eq!(tokens[1].kind, TokenKind::Operator(Operator::Plus));
		assert_eq!(
			tokens[2].kind,
			TokenKind::Parameter(ParameterKind::Positional(2))
		);
	}

	#[test]
	fn test_parameter_with_identifier() {
		let tokens = lex("name = $name").unwrap();
		assert_eq!(tokens.len(), 3);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(
			tokens[1].kind,
			TokenKind::Operator(Operator::Equal)
		);
		assert_eq!(
			tokens[2].kind,
			TokenKind::Parameter(ParameterKind::Named)
		);
	}
}
