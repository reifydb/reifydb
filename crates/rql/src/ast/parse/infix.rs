// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::{Operator, TokenKind};
use crate::ast::parse::error::unsupported_token_error;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{Ast, AstInfix, InfixOperator};
use reifydb_core::return_error;

impl Parser {
    pub(crate) fn parse_infix(&mut self, left: Ast) -> crate::Result<AstInfix> {
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

    pub(crate) fn parse_infix_operator(&mut self) -> crate::Result<InfixOperator> {
        let token = self.advance()?;
        match &token.kind {
            TokenKind::Operator(operator) => match operator {
                Operator::OpenParen => Ok(InfixOperator::Call(token)),
                Operator::Plus => Ok(InfixOperator::Add(token)),
                Operator::Minus => Ok(InfixOperator::Subtract(token)),
                Operator::Asterisk => Ok(InfixOperator::Multiply(token)),
                Operator::Slash => Ok(InfixOperator::Divide(token)),
                Operator::Percent => Ok(InfixOperator::Rem(token)),
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
                Operator::DoubleColon => Ok(InfixOperator::AccessNamespace(token)),
                Operator::As => Ok(InfixOperator::As(token)),
                Operator::And => Ok(InfixOperator::And(token)),
                Operator::Or => Ok(InfixOperator::Or(token)),
                Operator::Xor => Ok(InfixOperator::Xor(token)),
                _ => return_error!(unsupported_token_error(token)),
            },
            _ => return_error!(unsupported_token_error(token)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use crate::ast::Ast::{Infix, Literal};
    use crate::ast::AstLiteral;
    use crate::ast::lex::lex;
    use crate::ast::parse::infix::{AstInfix, InfixOperator};
    use crate::ast::parse::parse;

    #[test]
    fn test_as_one() {
        let tokens = lex("1 as one").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let infix = result[0].first_unchecked().as_infix();
        assert_eq!(infix.left.as_literal_number().value(), "1");
        assert!(matches!(infix.operator, InfixOperator::As(_)));
        assert_eq!(infix.right.as_identifier().value(), "one");
    }

    #[test]
    fn test_as_a() {
        let tokens = lex("1 as a").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let infix = result[0].first_unchecked().as_infix();
        assert_eq!(infix.left.as_literal_number().value(), "1");
        assert!(matches!(infix.operator, InfixOperator::As(_)));
        assert_eq!(infix.right.as_identifier().value(), "a");
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
    fn test_remainder() {
        let tokens = lex("1 % 2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Infix(AstInfix { left, operator, right, .. }) = result[0].first_unchecked() else {
            panic!()
        };

        let Literal(AstLiteral::Number(node)) = left.deref() else { panic!() };
        assert_eq!(node.value(), "1");

        assert!(matches!(operator, InfixOperator::Rem(_)));

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
}
