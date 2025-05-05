// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::lex::Keyword;
use crate::rql::lex::Operator::OpenParen;
use crate::rql::parse;
use crate::rql::parse::node::{Node, NodeFrom};
use crate::rql::parse::Parser;

impl Parser {
    pub(crate) fn parse_from(&mut self) -> parse::Result<NodeFrom> {
        let token = self.consume_keyword(Keyword::From)?;

        let source = if self.current()?.is_operator(OpenParen) {
            Node::Block(self.parse_block()?)
        } else {
            let ident = self.parse_identifier()?;
            Node::Identifier(ident)
        };
        Ok(NodeFrom { token, source: Box::new(source) })
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::lex::Keyword::From;
    use crate::rql::lex::{lex, TokenKind};
    use crate::rql::parse::node::Node;
    use crate::rql::parse::Parser;

    #[test]
    fn test_parse_from_identifier() {
        let tokens = lex("FROM users").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let from = result.as_from();

        assert_eq!(from.token.kind, TokenKind::Keyword(From));
        match *from.source {
            Node::Identifier(ref id) => assert_eq!(id.0.value(), "users"),
            _ => panic!("Expected Identifier node"),
        }
    }

    #[test]
    fn test_parse_from_block() {
        let tokens = lex("FROM ( FROM users SELECT name )").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let from = result.as_from();
        match *from.source {
            Node::Block(ref block) => {
                assert!(!block.nodes.is_empty(), "Block should not be empty");
                match &block.nodes[0] {
                    Node::From(from_inner) => match *from_inner.source {
                        Node::Identifier(ref id) => assert_eq!(id.0.value(), "users"),
                        _ => panic!("Expected Identifier inside nested FROM"),
                    },
                    _ => panic!("Expected From node inside Block"),
                }
            }
            _ => panic!("Expected Block node"),
        }
    }
}
