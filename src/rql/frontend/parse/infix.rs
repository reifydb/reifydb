// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{Operator, Token, TokenKind};
use crate::rql::frontend::parse;
use crate::rql::frontend::parse::node::Node;
use crate::rql::frontend::parse::{Error, Parser};

#[derive(Debug, PartialEq)]
pub(crate) enum InfixOperator {
    Add(Token),
    Arrow(Token),
    AccessPackage(Token),
    AccessProperty(Token),
    Assign(Token),
    Call(Token),
    Subtract(Token),
    Multiply(Token),
    Divide(Token),
    Modulo(Token),
    Equal(Token),
    NotEqual(Token),
    LessThan(Token),
    LessThanEqual(Token),
    GreaterThan(Token),
    GreaterThanEqual(Token),
    TypeAscription(Token),
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeInfix {
    pub(crate) token: Token,
    pub(crate) left: Box<Node>,
    pub(crate) operator: InfixOperator,
    pub(crate) right: Box<Node>,
}

impl Parser {
    pub(crate) fn parse_infix(&mut self, left: Node) -> parse::Result<NodeInfix> {
        let precedence = self.current_precedence()?;

        let operator = self.parse_infix_operator()?;

        let right = if let InfixOperator::Call(token) = &operator {
            Node::Tuple(self.parse_tuple_call(token.clone())?)
        // } else if let InfixOperator::Arrow(_) = &operator {
        // Node::Block(self.parse_block_inner(left.token())?)
        // unimplemented!()
        } else {
            self.parse_node(precedence)?
        };

        Ok(NodeInfix { token: left.token().clone(), left: Box::new(left), operator, right: Box::new(right) })
    }

    pub(crate) fn parse_infix_operator(&mut self) -> parse::Result<InfixOperator> {
        let token = self.advance()?;
        match &token.kind {
            TokenKind::Operator(operator) => match operator {
                Operator::OpenParen => Ok(InfixOperator::Call(token)),
                Operator::Plus => Ok(InfixOperator::Add(token)),
                Operator::Minus => Ok(InfixOperator::Subtract(token)),
                Operator::Asterisk => Ok(InfixOperator::Multiply(token)),
                Operator::Slash => Ok(InfixOperator::Divide(token)),
                Operator::Percent => Ok(InfixOperator::Modulo(token)),
                Operator::Equal => Ok(InfixOperator::Assign(token)),
                Operator::DoubleEqual => Ok(InfixOperator::Equal(token)),
                Operator::BangEqual => Ok(InfixOperator::NotEqual(token)),
                Operator::LeftAngle => Ok(InfixOperator::LessThan(token)),
                Operator::LeftAngleEqual => Ok(InfixOperator::LessThanEqual(token)),
                Operator::RightAngle => Ok(InfixOperator::GreaterThan(token)),
                Operator::RightAngleEqual => Ok(InfixOperator::GreaterThanEqual(token)),
                Operator::Colon => Ok(InfixOperator::TypeAscription(token)),
                Operator::Arrow => Ok(InfixOperator::Arrow(token)),
                Operator::Dot => Ok(InfixOperator::AccessProperty(token)),
                Operator::DoubleColon => Ok(InfixOperator::AccessPackage(token)),
                _ => Err(Error::unsupported(token)),
            },
            _ => Err(Error::unsupported(token)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use crate::rql::frontend::lex::lex;
    use crate::rql::frontend::parse::infix::{InfixOperator, NodeInfix};
    use crate::rql::frontend::parse::node::Node::{Infix, Literal};
    use crate::rql::frontend::parse::node::{NodeLiteral, NodeTuple};
    use crate::rql::frontend::parse::parse;

    #[test]
    fn test_add() {
        let tokens = lex("1 + 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Add(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_substract() {
        let tokens = lex("1 - 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Subtract(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_multiply() {
        let tokens = lex("1 * 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Multiply(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_divide() {
        let tokens = lex("1 / 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Divide(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_modulo() {
        let tokens = lex("1 % 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Modulo(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_greater_than() {
        let tokens = lex("1 > 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::GreaterThan(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_greater_than_or_equal() {
        let tokens = lex("1 >= 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::GreaterThanEqual(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_less_than() {
        let tokens = lex("1 < 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::LessThan(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_less_than_or_equal() {
        let tokens = lex("1 <= 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::LessThanEqual(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_equal() {
        let tokens = lex("1 == 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Equal(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_not_equal() {
        let tokens = lex("1 != 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { ref left, ref operator, ref right, .. }) = result[0] else { panic!() };

        let Literal(NodeLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::NotEqual(_)));

        let Literal(NodeLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_call_without_arguments() {
        let tokens = lex("test()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(NodeInfix { left, operator, right, .. }) = &result[0] else { panic!() };
        let identifier = left.as_identifier();
        assert_eq!(identifier.value(), "test");

        let InfixOperator::Call(_) = operator else { panic!() };

        let NodeTuple { nodes, .. } = right.as_tuple();
        assert_eq!(*nodes, vec![]);
    }

    #[test]
    fn test_call_with_argument() {
        let tokens = lex("test('elodie')").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let NodeInfix { left, operator, right, .. } = &result[0].as_infix();
        let identifier = left.as_identifier();
        assert_eq!(identifier.value(), "test");

        let InfixOperator::Call(_) = operator else { panic!() };

        let NodeTuple { nodes, .. } = right.as_tuple();
        assert_eq!(nodes.len(), 1);

        let Some(Literal(NodeLiteral::Text(arg_1))) = &nodes.first() else { panic!() };
        assert_eq!(arg_1.value(), "elodie");
    }

    #[test]
    fn test_call_package_function() {
        let tokens = lex("some_package::some_function()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let NodeInfix { left, operator, right, .. } = &result[0].as_infix();
        {
            let NodeInfix { left, operator, right, .. } = left.as_infix();

            let package = left.as_identifier();
            assert_eq!(package.value(), "some_package");

            assert!(matches!(operator, InfixOperator::AccessPackage(_)));

            let function = right.as_identifier();
            assert_eq!(function.value(), "some_function");
        }

        assert!(matches!(operator, InfixOperator::Call(_)));

        let NodeTuple { nodes, .. } = right.as_tuple();
        assert_eq!(*nodes, vec![]);
    }

    #[test]
    fn test_call_nested_package_function() {
        let tokens = lex("reify::db::log('Elodie')").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let NodeInfix { left, operator, right, .. } = &result[0].as_infix();
        {
            let NodeInfix { left, operator, right, .. } = left.as_infix();
            {
                let NodeInfix { left, operator, right, .. } = left.as_infix();
                let root_package = left.as_identifier();
                assert_eq!(root_package.value(), "reify");

                assert!(matches!(operator, InfixOperator::AccessPackage(_)));

                let root_package = right.as_identifier();
                assert_eq!(root_package.value(), "db");
            }

            assert!(matches!(operator, InfixOperator::AccessPackage(_)));

            let function = right.as_identifier();
            assert_eq!(function.value(), "log");
        }

        assert!(matches!(operator, InfixOperator::Call(_)));

        let NodeTuple { nodes, .. } = right.as_tuple();
        assert_eq!(nodes.len(), 1);

        let Literal(NodeLiteral::Text(node)) = &nodes[0] else { panic!() };
        assert_eq!(node.value(), "Elodie");
    }
    //
    // #[test]
    // fn test_instantiate_type_without_properties() {
    //     let mut ctx = Context::testing();
    //     let tokens = lex(&mut ctx, "Point()").unwrap();
    //     let result = parse(&mut ctx, tokens).unwrap();
    //     assert_eq!(result.len(), 1);
    //
    //     let block = result[0].as_infix();
    // }
    //
    // #[test]
    // fn test_call_function_with_lambda() {
    //     let mut ctx = Context::testing();
    //     let tokens = lex(&mut ctx, "test('elodie'){ true }").unwrap();
    //     let result = parse(&mut ctx, tokens).unwrap();
    //     assert_eq!(result.len(), 1);
    //
    //     let NodeInfix { left, operator, right, .. } = &result[0].as_infix();
    //     let call = left.as_infix();
    //     {
    //         let NodeInfix { left, operator, right, .. } = call;
    //         let identifier = left.as_identifier();
    //         assert_eq!(ctx.str_get(identifier.value()), "test");
    //         let InfixOperator::Call(_) = operator else { panic!() };
    //
    //         let TupleNode { nodes, .. } = right.as_tuple();
    //         assert_eq!(nodes.len(), 1);
    //         let Some(Literal(NodeLiteral::String(arg_1))) = &nodes.first() else { panic!() };
    //         assert_eq!(ctx.str_get(arg_1.value()), "elodie");
    //     }
    //
    //     let InfixOperator::LambdaCall(_) = operator else { panic!() };
    //
    //     let block = right.as_block();
    //     assert_eq!(block.nodes.len(), 1);
    //
    //     let Literal(NodeLiteral::Boolean(boolean_node)) = &block.nodes[0] else { panic!() };
    //     assert!(boolean_node.value())
    // }
    //
    // #[test]
    // fn test_property_access_and_comparison() {
    //     let mut ctx = Context::testing();
    //     let tokens = lex(&mut ctx, "p.x == 1").unwrap();
    //     let result = parse(&mut ctx, tokens).unwrap();
    //     assert_eq!(result.len(), 1);
    //
    //     let NodeInfix { left, operator, right, .. } = &result[0].as_infix();
    //     {
    //         let NodeInfix { left, operator, right, .. } = left.as_infix();
    //         let left = left.as_identifier();
    //         assert_eq!(ctx.str_get(left.value()), "p");
    //
    //         assert!(matches!(operator, InfixOperator::AccessProperty(_)));
    //
    //         let right = right.as_identifier();
    //         assert_eq!(ctx.str_get(right.value()), "x");
    //     }
    //
    //     assert!(matches!(operator, InfixOperator::Equal(_)));
    //
    //     let NodeLiteral::Number(right) = right.as_literal() else { panic!() };
    //     assert_eq!(ctx.str_get(right.value()), "1");
    // }
}
