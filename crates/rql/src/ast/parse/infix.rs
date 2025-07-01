// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::{Operator, TokenKind};
use crate::ast::parse::{Error, Parser, Precedence};
use crate::ast::{parse, Ast, AstInfix, InfixOperator};

impl Parser {
    pub(crate) fn parse_infix(&mut self, left: Ast) -> parse::Result<AstInfix> {
        let precedence = self.current_precedence()?;

        let operator = self.parse_infix_operator()?;

        let right = if let InfixOperator::Call(token) = &operator {
            Ast::Tuple(self.parse_tuple_call(token.clone())?)
        } else if let InfixOperator::As(_token) = &operator {
            self.parse_node(Precedence::None)?
        } else {
            self.parse_node(precedence)?
        };

        Ok(AstInfix {
            token: left.token().clone(),
            left: Box::new(left),
            operator,
            right: Box::new(right),
        })
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
                Operator::Dot => Ok(InfixOperator::AccessTable(token)),
                Operator::DoubleColon => Ok(InfixOperator::AccessExtension(token)),
                Operator::As => Ok(InfixOperator::As(token)),
                _ => Err(Error::unsupported(token)),
            },
            _ => Err(Error::unsupported(token)),
        }
    }
}

#[cfg(test)]
mod tests {
	use std::ops::Deref;

	use crate::ast::lex::lex;
	use crate::ast::parse::infix::{AstInfix, InfixOperator};
	use crate::ast::parse::parse;
	use crate::ast::Ast::{Infix, Literal};
	use crate::ast::{AstLiteral, AstTuple};

	#[test]
    fn test_as() {
        let tokens = lex("1 as one").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let infix = result[0].first_unchecked().as_infix();
        assert_eq!(infix.left.as_literal_number().value(), "1");
        assert!(matches!(infix.operator, InfixOperator::As(_)));
        assert_eq!(infix.right.as_identifier().value(), "one");
    }

    #[test]
    fn test_add() {
        let tokens = lex("1 + 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Add(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_cast_infix() {
        let tokens = lex("cast(-1, int1) < cast(1, int16)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstInfix { left, operator, right, .. } = result[0].first_unchecked().as_infix();
        assert!(matches!(operator, InfixOperator::LessThan(_)));

        left.as_cast();
        right.as_cast();
    }

    #[test]
    fn test_subtract() {
        let tokens = lex("1 - 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Subtract(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_subtract_negative() {
        let tokens = lex("-1 -2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstInfix { left, operator, right, .. } = result[0].first_unchecked().as_infix();

        let left = left.as_literal_number();
        assert_eq!(left.value(), "-1");

        assert!(matches!(operator, InfixOperator::Subtract(_)));

        let right_number = right.as_literal_number();
        assert_eq!(right_number.value(), "2");
    }

    #[test]
    fn test_multiply() {
        let tokens = lex("1 * 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Multiply(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_divide() {
        let tokens = lex("1 / 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Divide(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_modulo() {
        let tokens = lex("1 % 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Modulo(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_greater_than() {
        let tokens = lex("1 > 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::GreaterThan(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_greater_than_or_equal() {
        let tokens = lex("1 >= 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::GreaterThanEqual(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_less_than() {
        let tokens = lex("1 < 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::LessThan(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_less_than_or_equal() {
        let tokens = lex("1 <= 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::LessThanEqual(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_equal() {
        let tokens = lex("1 == 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Equal(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_not_equal() {
        let tokens = lex("1 != 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::NotEqual(_)));

        let Literal(AstLiteral::Number(node)) = right.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_call_without_arguments() {
        let tokens = lex("test()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = &result[0].first_unchecked() else {
            panic!()
        };
        let identifier = left.as_identifier();
        assert_eq!(identifier.value(), "test");

        let InfixOperator::Call(_) = operator else { panic!() };

        let AstTuple { nodes, .. } = right.as_tuple();
        assert_eq!(*nodes, vec![]);
    }

    #[test]
    fn test_call_with_argument() {
        let tokens = lex("test('elodie')").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstInfix { left, operator, right, .. } = &result[0].first_unchecked().as_infix();
        let identifier = left.as_identifier();
        assert_eq!(identifier.value(), "test");

        let InfixOperator::Call(_) = operator else { panic!() };

        let AstTuple { nodes, .. } = right.as_tuple();
        assert_eq!(nodes.len(), 1);

        let Some(Literal(AstLiteral::Text(arg_1))) = &nodes.first() else { panic!() };
        assert_eq!(arg_1.value(), "elodie");
    }

    #[test]
    fn test_call_extension_function() {
        let tokens = lex("some_extension::some_function()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstInfix { left, operator, right, .. } = &result[0].first_unchecked().as_infix();
        {
            let AstInfix { left, operator, right, .. } = left.as_infix();

            let package = left.as_identifier();
            assert_eq!(package.value(), "some_extension");

            assert!(matches!(operator, InfixOperator::AccessExtension(_)));

            let function = right.as_identifier();
            assert_eq!(function.value(), "some_function");
        }

        assert!(matches!(operator, InfixOperator::Call(_)));

        let AstTuple { nodes, .. } = right.as_tuple();
        assert_eq!(*nodes, vec![]);
    }

    #[test]
    fn test_call_nested_package_function() {
        let tokens = lex("reify::db::log('Elodie')").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstInfix { left, operator, right, .. } = &result[0].first_unchecked().as_infix();
        {
            let AstInfix { left, operator, right, .. } = left.as_infix();
            {
                let AstInfix { left, operator, right, .. } = left.as_infix();
                let root_package = left.as_identifier();
                assert_eq!(root_package.value(), "reify");

                assert!(matches!(operator, InfixOperator::AccessExtension(_)));

                let root_package = right.as_identifier();
                assert_eq!(root_package.value(), "db");
            }

            assert!(matches!(operator, InfixOperator::AccessExtension(_)));

            let function = right.as_identifier();
            assert_eq!(function.value(), "log");
        }

        assert!(matches!(operator, InfixOperator::Call(_)));

        let AstTuple { nodes, .. } = right.as_tuple();
        assert_eq!(nodes.len(), 1);

        let Literal(AstLiteral::Text(node)) = &nodes[0] else { panic!() };
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
    //     let block = result[0].first_unchecked().as_infix();
    // }
    //
    // #[test]
    // fn test_call_function_with_lambda() {
    //     let mut ctx = Context::testing();
    //     let tokens = lex(&mut ctx, "test('elodie'){ true }").unwrap();
    //     let result = parse(&mut ctx, tokens).unwrap();
    //     assert_eq!(result.len(), 1);
    //
    //     let NodeInfix { left, operator, right, .. } = &result[0].first_unchecked().as_infix();
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
    //     let NodeInfix { left, operator, right, .. } = &result[0].first_unchecked().as_infix();
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
