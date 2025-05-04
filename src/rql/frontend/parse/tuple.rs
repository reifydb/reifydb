// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{Operator, Separator, Token, TokenKind};
use crate::rql::frontend::parse;
use crate::rql::frontend::parse::node::NodeTuple;
use crate::rql::frontend::parse::{Parser, Precedence};
use Operator::CloseParen;

impl Parser {
    pub(crate) fn parse_tuple(&mut self) -> parse::Result<NodeTuple> {
        let token = self.consume_operator(Operator::OpenParen)?;
        self.parse_tuple_call(token)
    }

    pub(crate) fn parse_tuple_call(&mut self, operator: Token) -> parse::Result<NodeTuple> {
        let mut nodes = Vec::new();
        loop {
            self.skip_new_line()?;

            if self.current()?.is_operator(CloseParen) {
                break;
            }
            nodes.push(self.parse_node(Precedence::None)?);
            self.consume_if(TokenKind::Separator(Separator::Comma))?;
        }

        self.consume_operator(CloseParen)?;
        Ok(NodeTuple { token: operator, nodes })
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::lex;
    use crate::rql::frontend::parse::infix::{InfixOperator, NodeInfix};
    use crate::rql::frontend::parse::node::Node::{Identifier, Infix, Literal, Type};
    use crate::rql::frontend::parse::node::NodeLiteral::Number;
    use crate::rql::frontend::parse::node::{NodeLiteral, NodeType};
    use crate::rql::frontend::parse::parse;

    #[test]
    fn empty_tuple() {
        let tokens = lex("()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();
        assert_eq!(node.nodes, vec![]);
    }

    #[test]
    fn tuple_with_number() {
        let tokens = lex("(9924)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Literal(Number(number)) = &node else { panic!() };
        assert_eq!(number.value(), "9924");
    }

    #[test]
    fn nested_tuple() {
        let tokens = lex("(1 * ( 2 + 3 ))").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &node else { panic!() };

        let Literal(Number(left)) = &left.as_ref() else { panic!() };
        assert_eq!(left.value(), "1");

        let node = right.as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let NodeInfix { left, operator, right, .. } = &node.as_infix();

        let Literal(Number(left)) = &left.as_ref() else { panic!() };
        assert_eq!(left.value(), "2");

        let Literal(Number(right)) = &right.as_ref() else { panic!() };
        assert_eq!(right.value(), "3");
    }

    #[test]
    fn tuple_with_identifier() {
        let tokens = lex("(u)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = &result[0].as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Identifier(node) = node else { panic!() };
        assert_eq!(node.value(), "u");
    }

    #[test]
    fn tuple_with_identifier_and_type() {
        let tokens = lex("(u: Bool)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &node else { panic!() };

        let identifier = &left.as_identifier();
        assert_eq!(identifier.value(), "u");

        let Type(NodeType::Boolean(_)) = right.as_ref() else { panic!() };
    }

    #[test]
    fn tuple_with_multiple_identifiers() {
        let tokens = lex("(u,v)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();

        let Some(Identifier(u_node)) = &node.nodes.first() else { panic!() };
        assert_eq!(u_node.value(), "u");

        let Some(Identifier(v_node)) = &node.nodes.last() else { panic!() };
        assert_eq!(v_node.value(), "v");
    }

    #[test]
    fn tuple_with_identifiers_and_types() {
        let tokens = lex("(u: Bool, v: Text)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();

        let Some(u_node) = node.nodes.first() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &u_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "u");
        let Type(NodeType::Boolean(_)) = right.as_ref() else { panic!() };

        let Some(v_node) = node.nodes.last() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &v_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "v");
        let Type(NodeType::Text(_)) = right.as_ref() else { panic!() };
    }

    #[test]
    fn tuple_with_identifiers_and_declaration() {
        let tokens = lex("(u = 1, v = 2)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();

        let Some(u_node) = node.nodes.first() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &u_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "u");
        assert!(matches!(operator, InfixOperator::Assign(_)));
        let Literal(NodeLiteral::Number(number)) = right.as_ref() else { panic!() };
        assert_eq!(number.value(), "1");

        let Some(v_node) = node.nodes.last() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &v_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "v");
        assert!(matches!(operator, InfixOperator::Assign(_)));
        let Literal(NodeLiteral::Number(number)) = right.as_ref() else { panic!() };
        assert_eq!(number.value(), "2");
    }

    #[test]
    fn multiline_tuple() {
        let tokens = lex(r#"(
        u: Bool,
        v: Text
        )"#)
        .unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].as_tuple();

        let Some(u_node) = node.nodes.first() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &u_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "u");
        let Type(NodeType::Boolean(_)) = right.as_ref() else { panic!() };

        let Some(v_node) = node.nodes.last() else { panic!() };
        let Infix(NodeInfix { left, operator, right, .. }) = &v_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "v");
        let Type(NodeType::Text(_)) = right.as_ref() else { panic!() };
    }
}
