// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::lex::TokenKind;
use crate::rql::ast::AstIdentifier;
use crate::rql::parse;
use crate::rql::parse::Parser;

impl Parser {
    pub(crate) fn parse_identifier(&mut self) -> parse::Result<AstIdentifier> {
        let token = self.consume(TokenKind::Identifier)?;
        Ok(AstIdentifier(token))
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::lex::lex;
    use crate::rql::ast::Ast::Identifier;
    use crate::rql::ast::AstIdentifier;
    use crate::rql::parse::parse;

    #[test]
    fn identifier() {
        let tokens = lex("x").unwrap();
        let mut result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Identifier(AstIdentifier(token)) = result.pop().unwrap() else { panic!() };
        assert_eq!(token.value(), "x");
    }

    #[test]
    fn identifier_with_underscore() {
        let tokens = lex("some_identifier").unwrap();
        let mut result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Identifier(AstIdentifier(token)) = result.pop().unwrap() else { panic!() };
        assert_eq!(token.value(), "some_identifier");
    }
}
