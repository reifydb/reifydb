// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use error::Error;

use crate::rql::frontend::lex::keyword::Keyword;
use crate::rql::frontend::lex::operator::Operator;
use nom::Parser;
use nom_locate::LocatedSpan;

mod error;
mod keyword;
mod operator;
mod separator;

pub type Span<'a> = LocatedSpan<&'a str>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token<'a> {
    pub kind: TokenKind,
    pub span: Span<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Operator(Operator),
    Keyword(Keyword),
    // Identifier(),
    // Text
    // Number
    // Separator
}

// fn identifier(input: &str) -> IResult<&str, Token> {
//     let (input, ident) =
//         recognize(pair(take_while1(is_identifier_start), take_while1(is_identifier_char)))
//             .parse(input)?;
//
//     let keywords = ["select", "from", "where", "and", "or", "limit"];
//
//     if keywords.contains(&ident.to_lowercase().as_str()) {
//         Ok((input, Token::Keyword(ident)))
//     } else {
//         Ok((input, Token::Identifier(ident)))
//     }
// }
//
// fn number(input: &str) -> IResult<&str, Token> {
//     map(recognize(take_while1(|c: char| c.is_digit(10))), Token::Number).parse(input)
// }
//
// fn string_literal(input: &str) -> IResult<&str, Token> {
//     map(delimited(char('\''), is_not("'"), char('\'')), Token::StringLiteral).parse(input)
// }
//
// fn operator(input: &str) -> IResult<&str, Token> {
//     map(alt((tag("="), tag("!="), tag("<"), tag(">"), tag("<="), tag(">="))), Token::Operator)
//         .parse(input)
// }
//
// fn punctuation(input: &str) -> IResult<&str, Token> {
//     map(one_of("(),;"), Token::Punctuation).parse(input)
// }
//
// fn token(input: &str) -> IResult<&str, Token> {
//     preceded(multispace0(), alt((identifier, number, string_literal, operator, punctuation)))
//         .parse(input)
// }
//
// pub fn tokenize(input: &str) -> IResult<&str, Vec<Token>> {
//     many0(token).parse(input)
// }
