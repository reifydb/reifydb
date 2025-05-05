// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::lex::TokenKind;
use crate::rql::parse;
use crate::rql::parse::node::NodeIdentifier;
use crate::rql::parse::Parser;

impl Parser {
    pub(crate) fn parse_identifier(&mut self) -> parse::Result<NodeIdentifier> {
        let token = self.consume(TokenKind::Identifier)?;
        Ok(NodeIdentifier(token))
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::lex::lex;
    use crate::rql::parse::node::Node::Identifier;
    use crate::rql::parse::node::NodeIdentifier;
    use crate::rql::parse::parse;

    #[test]
    fn identifier() {
        let tokens = lex("x").unwrap();
        let mut result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Identifier(NodeIdentifier(token)) = result.pop().unwrap() else { panic!() };
        assert_eq!(token.value(), "x");
    }

    #[test]
    fn identifier_with_underscore() {
        let tokens = lex("some_identifier").unwrap();
        let mut result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Identifier(NodeIdentifier(token)) = result.pop().unwrap() else { panic!() };
        assert_eq!(token.value(), "some_identifier");
    }
}
