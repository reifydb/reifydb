// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::TokenKind;
use crate::rql::frontend::parse;
use crate::rql::frontend::parse::node::NodeIdentifier;
use crate::rql::frontend::parse::Parser;

impl Parser {
    pub(crate) fn parse_identifier(&mut self) -> parse::Result<NodeIdentifier> {
        let token = self.consume(TokenKind::Identifier)?;
        Ok(NodeIdentifier(token))
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::lex;
    use crate::rql::frontend::parse::node::Node::Identifier;
    use crate::rql::frontend::parse::node::NodeIdentifier;
    use crate::rql::frontend::parse::parse;

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
