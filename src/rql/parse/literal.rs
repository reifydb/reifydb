// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::lex::Literal;
use crate::rql::parse;
use crate::rql::parse::node::{NodeLiteral, NodeLiteralBoolean, NodeLiteralNumber, NodeLiteralText, NodeLiteralUndefined};
use crate::rql::parse::Parser;
use std::str::FromStr;

impl Parser {
    pub(crate) fn parse_literal_number(&mut self) -> parse::Result<NodeLiteral> {
        let token = self.consume_literal(Literal::Number)?;
        Ok(NodeLiteral::Number(NodeLiteralNumber(token)))
    }

    pub(crate) fn parse_literal_text(&mut self) -> parse::Result<NodeLiteral> {
        let token = self.consume_literal(Literal::Text)?;
        Ok(NodeLiteral::Text(NodeLiteralText(token)))
    }

    pub(crate) fn parse_literal_true(&mut self) -> parse::Result<NodeLiteral> {
        let token = self.consume_literal(Literal::True)?;
        Ok(NodeLiteral::Boolean(NodeLiteralBoolean(token)))
    }

    pub(crate) fn parse_literal_false(&mut self) -> parse::Result<NodeLiteral> {
        let token = self.consume_literal(Literal::False)?;
        Ok(NodeLiteral::Boolean(NodeLiteralBoolean(token)))
    }

    pub(crate) fn parse_literal_undefined(&mut self) -> parse::Result<NodeLiteral> {
        let token = self.consume_literal(Literal::Undefined)?;
        Ok(NodeLiteral::Undefined(NodeLiteralUndefined(token)))
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::lex::lex;
    use crate::rql::parse::node::Node::Literal;
    use crate::rql::parse::node::NodeLiteral;
    use crate::rql::parse::parse;

    #[test]
    fn test_text() {
        let tokens = lex("'ElodiE'").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(NodeLiteral::Text(node)) = &result[0] else { panic!() };
        assert_eq!(node.value(), "ElodiE");
    }

    #[test]
    fn test_number_42() {
        let tokens = lex("42").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(NodeLiteral::Number(node)) = &result[0] else { panic!() };
        assert_eq!(node.value(), "42");
    }

    #[test]
    fn test_true() {
        let tokens = lex("true").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(NodeLiteral::Boolean(node)) = &result[0] else { panic!() };
        assert_eq!(node.value(), true);
    }

    #[test]
    fn test_false() {
        let tokens = lex("false").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(NodeLiteral::Boolean(node)) = &result[0] else { panic!() };
        assert_eq!(node.value(), false);
    }
}
