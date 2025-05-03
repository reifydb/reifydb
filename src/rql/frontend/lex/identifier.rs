// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{Span, Token, TokenKind};
use nom::bytes::complete::take_while1;
use nom::bytes::take_while;
use nom::combinator::{complete, recognize};
use nom::sequence::pair;
use nom::{IResult, Parser};

pub(crate) fn parse_identifier(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(pair(take_while1(is_identifier_start), take_while(is_identifier_char)))).parse(input)?;
    Ok((rest, Token { kind: TokenKind::Identifier, span }))
}

fn is_identifier_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::identifier::parse_identifier;
    use crate::rql::frontend::lex::TokenKind::Identifier;
    use crate::rql::frontend::lex::{Span, Token};

    #[test]
    fn test_identifier() {
        let (_rest, result) = parse_identifier(Span::new("user_referral")).unwrap();
        assert_eq!(result, Token { kind: Identifier, span: Span::new("user_referral") });
    }
}
