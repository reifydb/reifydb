// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::{Token, TokenKind, as_span};
use nom::bytes::complete::take_while1;
use nom::bytes::take_while;
use nom::combinator::{complete, recognize};
use nom::sequence::pair;
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

pub(crate) fn parse_identifier(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let (rest, span) =
        complete(recognize(pair(take_while1(is_identifier_start), take_while(is_identifier_char))))
            .parse(input)?;
    Ok((rest, Token { kind: TokenKind::Identifier, span: as_span(span) }))
}

fn is_identifier_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::TokenKind;
    use crate::ast::lex::identifier::parse_identifier;
    use nom_locate::LocatedSpan;

    #[test]
    fn test_identifier() {
        let (_rest, result) = parse_identifier(LocatedSpan::new("user_referral")).unwrap();
        assert_eq!(result.kind, TokenKind::Identifier);
        assert_eq!(&result.span.fragment, "user_referral");
    }
}
