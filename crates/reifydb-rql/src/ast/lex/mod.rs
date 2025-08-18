// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use keyword::Keyword;
use nom::{
	IResult, Parser, branch::alt, character::multispace0,
	combinator::complete, multi::many0, sequence::preceded,
};
use nom_locate::LocatedSpan;
pub use operator::Operator;
pub use parameter::ParameterKind;
use reifydb_core::{
	OwnedFragment, StatementColumn, StatementLine,
	result::error::diagnostic::ast,
};
pub use separator::Separator;

use crate::ast::lex::{
	TokenKind::EOF, identifier::parse_identifier, keyword::parse_keyword,
	literal::parse_literal, operator::parse_operator,
	parameter::parse_parameter, separator::parse_separator,
};

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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Literal {
	False,
	Number,
	Temporal,
	Text,
	True,
	Undefined,
}

pub fn lex<'a>(
	input: impl Into<LocatedSpan<&'a str>>,
) -> crate::Result<Vec<Token>> {
	match many0(token).parse(input.into()) {
		Ok((_, tokens)) => Ok(tokens),
		Err(err) => Err(reifydb_core::error::Error(ast::lex_error(
			format!("{}", err),
		))),
	}
}

fn token(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
	complete(preceded(
		multispace0(),
		alt((
			parse_keyword,
			parse_operator,
			parse_literal,
			parse_parameter, // Must come before identifier
			parse_identifier,
			parse_separator,
		)),
	))
	.parse(input)
}

pub(crate) fn as_fragment(value: LocatedSpan<&str>) -> OwnedFragment {
	OwnedFragment::Statement {
		column: StatementColumn(value.get_column() as u32),
		line: StatementLine(value.location_line()),
		text: value.fragment().to_string(),
	}
}

#[cfg(test)]
mod tests {
	use TokenKind::Literal;

	use super::*;
	use crate::ast::lex::Literal::{Number, Text};

	fn fragment(s: &str) -> LocatedSpan<&str> {
		LocatedSpan::new(s)
	}

	#[test]
	fn test_keyword() {
		let (_rest, token) = token(fragment("MAP")).unwrap();
		assert_eq!(token.kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(token.fragment.fragment(), "MAP");
	}

	#[test]
	fn test_identifier() {
		let (_rest, token) = token(fragment("my_var123")).unwrap();
		assert_eq!(token.kind, TokenKind::Identifier);
		assert_eq!(token.fragment.fragment(), "my_var123");
	}

	#[test]
	fn test_number() {
		let (_rest, token) = token(fragment("42")).unwrap();
		assert_eq!(token.kind, Literal(Number));
		assert_eq!(token.fragment.fragment(), "42");
	}

	#[test]
	fn test_number_negative() {
		let (rest, token) = token(fragment("-42")).unwrap();
		assert_eq!(token.kind, TokenKind::Operator(Operator::Minus));
		assert_eq!(token.fragment.fragment(), "-");
		assert_eq!(rest.fragment().to_string(), "42");
	}

	#[test]
	fn test_text() {
		let (_rest, token) = token(fragment("'hello world'")).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(token.fragment.fragment(), "hello world");
	}

	#[test]
	fn test_operator() {
		let (_rest, token) = token(fragment("+")).unwrap();
		assert_eq!(token.kind, TokenKind::Operator(Operator::Plus));
		assert_eq!(token.fragment.fragment(), "+");
	}

	#[test]
	fn test_separator() {
		let (_rest, token) = token(fragment(",")).unwrap();
		assert_eq!(token.kind, TokenKind::Separator(Separator::Comma));
		assert_eq!(token.fragment.fragment(), ",");
	}

	#[test]
	fn test_skips_whitespace() {
		let (_rest, token) = token(fragment("   MAP")).unwrap();
		assert_eq!(token.kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(token.fragment.fragment(), "MAP");
	}

	#[test]
	fn test_desc() {
		let (_rest, token) = token(fragment("DESC")).unwrap();
		assert_eq!(token.kind, TokenKind::Keyword(Keyword::Desc));
		assert_eq!(token.fragment.fragment(), "DESC");
	}

	#[test]
	fn test_a() {
		let (_rest, token) = token(fragment("a")).unwrap();
		assert_eq!(token.kind, TokenKind::Identifier);
		assert_eq!(token.fragment.fragment(), "a");
	}

	#[test]
	fn test_parameter_positional() {
		let (_rest, token) = token(fragment("$1")).unwrap();
		assert_eq!(
			token.kind,
			TokenKind::Parameter(ParameterKind::Positional(1))
		);
		assert_eq!(token.fragment.fragment(), "$1");
	}

	#[test]
	fn test_parameter_named() {
		let (_rest, token) = token(fragment("$user_id")).unwrap();
		assert_eq!(
			token.kind,
			TokenKind::Parameter(ParameterKind::Named)
		);
		assert_eq!(token.fragment.fragment(), "$user_id");
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
