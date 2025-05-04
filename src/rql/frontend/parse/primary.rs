// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::Literal::{False, Number, Text, True, Undefined};
use crate::rql::frontend::lex::Separator::NewLine;
use crate::rql::frontend::lex::{Operator, TokenKind};
use crate::rql::frontend::parse;
use crate::rql::frontend::parse::node::{Node, NodePrefix, PrefixOperator};
use crate::rql::frontend::parse::{Error, Parser, Precedence};

impl Parser {
    pub(crate) fn parse_primary(&mut self) -> parse::Result<Node> {
        loop {
            if self.is_eof() {
                return Ok(Node::Nop);
            }

            let is_new_line = self.current()?.is_separator(NewLine);
            if !is_new_line {
                break;
            }
            let _ = self.advance()?;
        }

        let current = self.current()?;
        match &current.kind {
            TokenKind::Operator(operator) => match operator {
                Operator::Plus | Operator::Minus | Operator::Bang => {
                    let operator = self.parse_prefix_operator()?;
                    Ok(Node::Prefix(NodePrefix { operator, node: Box::new(self.parse_node(Precedence::None)?) }))
                }
                Operator::OpenParen => Ok(Node::Tuple(self.parse_tuple()?)),
                _ => Err(Error::unsupported(self.advance()?)),
            },
            TokenKind::Keyword(keyword) => match keyword {
                // Keyword::From => Ok(Node::From(self.parse_from()?)),
                _ => Err(Error::unsupported(self.advance()?)),
            },
            _ => match current {
                _ if current.is_literal(Number) => Ok(Node::Literal(self.parse_literal_number()?)),
                _ if current.is_literal(True) => Ok(Node::Literal(self.parse_literal_true()?)),
                _ if current.is_literal(False) => Ok(Node::Literal(self.parse_literal_false()?)),
                _ if current.is_literal(Text) => Ok(Node::Literal(self.parse_literal_text()?)),
                _ if current.is_literal(Undefined) => Ok(Node::Literal(self.parse_literal_undefined()?)),
                _ if current.is_identifier() => match self.parse_type() {
                    Ok(node) => Ok(Node::Type(node)),
                    Err(_) => Ok(Node::Identifier(self.parse_identifier()?)),
                },
                _ => Err(Error::unsupported(self.advance()?)),
            },
        }
    }

    pub(crate) fn parse_prefix_operator(&mut self) -> parse::Result<PrefixOperator> {
        let token = self.advance()?;
        match &token.kind {
            TokenKind::Operator(operator) => match operator {
                Operator::Plus => Ok(PrefixOperator::Plus(token)),
                Operator::Minus => Ok(PrefixOperator::Negate(token)),
                Operator::Bang => Ok(PrefixOperator::Not(token)),
                _ => Err(Error::unsupported(token)),
            },
            _ => Err(Error::unsupported(token)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::lex;
    use crate::rql::frontend::parse::node::Node::Literal;
    use crate::rql::frontend::parse::node::{Node, NodeLiteral, NodePrefix, PrefixOperator};
    use crate::rql::frontend::parse::parse;
    use std::ops::Deref;

    #[test]
    fn test_plus() {
        let tokens = lex("+2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Node::Prefix(NodePrefix { ref operator, ref node }) = result[0] else { panic!() };
        assert!(matches!(*operator, PrefixOperator::Plus(_)));

        let Literal(NodeLiteral::Number(node)) = node.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_negate() {
        let tokens = lex("-2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Node::Prefix(NodePrefix { ref operator, ref node }) = result[0] else { panic!() };
        assert!(matches!(*operator, PrefixOperator::Negate(_)));

        let Literal(NodeLiteral::Number(node)) = node.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_not() {
        let tokens = lex("!false").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Node::Prefix(NodePrefix { ref operator, ref node }) = result[0] else { panic!() };
        assert!(matches!(*operator, PrefixOperator::Not(_)));

        let Literal(NodeLiteral::Boolean(node)) = node.deref() else { panic!() };
        assert_eq!(node.value(), false);
    }
}
