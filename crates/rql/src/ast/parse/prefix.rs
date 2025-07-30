// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Literal::Number;
use crate::ast::lex::Operator;
use crate::ast::parse::error::unsupported_token_error;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{
    Ast, AstLiteral, AstLiteralNumber, AstPrefix, AstPrefixOperator, Token, TokenKind,
};
use reifydb_core::OwnedSpan;
use reifydb_core::return_error;

impl Parser {
    pub(crate) fn parse_prefix(&mut self) -> crate::Result<Ast> {
        let operator = self.parse_prefix_operator()?;

        // NOT operator should have lower precedence than comparison operators
        // to allow expressions like "not price == 150" to parse as "not (price == 150)"
        let precedence = match operator {
            AstPrefixOperator::Not(_) => Precedence::Assignment, // Much lower than comparisons
            _ => Precedence::Prefix, // Keep existing high precedence for +/- operators
        };

        let expr = self.parse_node(precedence)?;

        if matches!(operator, AstPrefixOperator::Negate(_)) {
            if let Ast::Literal(AstLiteral::Number(literal)) = &expr {
                return Ok(Ast::Literal(AstLiteral::Number(AstLiteralNumber(Token {
                    kind: TokenKind::Literal(Number),
                    span: OwnedSpan {
                        column: operator.token().span.column,
                        line: operator.token().span.line,
                        fragment: format!("-{}", literal.0.span.fragment),
                    },
                }))));
            }
        }

        Ok(Ast::Prefix(AstPrefix { operator, node: Box::new(expr) }))
    }

    fn parse_prefix_operator(&mut self) -> crate::Result<AstPrefixOperator> {
        let token = self.advance()?;
        match &token.kind {
            TokenKind::Operator(operator) => match operator {
                Operator::Plus => Ok(AstPrefixOperator::Plus(token)),
                Operator::Minus => Ok(AstPrefixOperator::Negate(token)),
                Operator::Bang => Ok(AstPrefixOperator::Not(token)),
                Operator::Not => Ok(AstPrefixOperator::Not(token)),
                _ => return_error!(unsupported_token_error(token)),
            },
            _ => return_error!(unsupported_token_error(token)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Ast::Literal;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::ast::{Ast, AstLiteral, AstLiteralNumber, AstPrefix, AstPrefixOperator};
    use std::ops::Deref;

    #[test]
    fn test_negative_number() {
        let tokens = lex("-2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Number(AstLiteralNumber(token))) = &result[0].first_unchecked()
        else {
            panic!()
        };
        assert_eq!(token.value(), "-2");
    }

    #[test]
    fn test_group_plus() {
        let tokens = lex("+(2)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { operator, node }) = result[0].first_unchecked() else {
            panic!()
        };
        assert!(matches!(*operator, AstPrefixOperator::Plus(_)));

        let Ast::Tuple(tuple) = node.deref() else { panic!() };
        let Literal(AstLiteral::Number(node)) = &tuple.nodes.first().unwrap() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_group_negate() {
        let tokens = lex("-(2)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { operator, node }) = result[0].first_unchecked() else {
            panic!()
        };
        assert!(matches!(*operator, AstPrefixOperator::Negate(_)));

        let Ast::Tuple(tuple) = node.deref() else { panic!() };
        let Literal(AstLiteral::Number(node)) = &tuple.nodes.first().unwrap() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_group_negate_negative_number() {
        let tokens = lex("-(-2)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { operator, node }) = result[0].first_unchecked() else {
            panic!()
        };
        assert!(matches!(*operator, AstPrefixOperator::Negate(_)));

        let Ast::Tuple(tuple) = node.deref() else { panic!() };
        let Literal(AstLiteral::Number(AstLiteralNumber(token))) = &tuple.nodes.first().unwrap()
        else {
            panic!()
        };
        assert_eq!(token.value(), "-2");
    }

    #[test]
    fn test_not_false() {
        let tokens = lex("!false").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { operator, node }) = result[0].first_unchecked() else {
            panic!()
        };
        assert!(matches!(*operator, AstPrefixOperator::Not(_)));

        let Literal(AstLiteral::Boolean(node)) = node.deref() else { panic!() };
        assert!(!node.value());
    }

    #[test]
    fn test_not_word_false() {
        let tokens = lex("not false").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { operator, node }) = result[0].first_unchecked() else {
            panic!()
        };
        assert!(matches!(*operator, AstPrefixOperator::Not(_)));

        let Literal(AstLiteral::Boolean(node)) = node.deref() else { panic!() };
        assert!(!node.value());
    }

    #[test]
    fn test_not_comparison_precedence() {
        let tokens = lex("not x == 5").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        // Should parse as: not (x == 5), not (not x) == 5
        let Ast::Prefix(AstPrefix { operator, node }) = result[0].first_unchecked() else {
            panic!("Expected prefix expression, got {:?}", result[0].first_unchecked())
        };
        assert!(matches!(*operator, AstPrefixOperator::Not(_)));

        // The inner expression should be a comparison (x == 5)
        let Ast::Infix(inner) = node.deref() else {
            panic!("Expected infix comparison inside NOT, got {:?}", node.deref())
        };

        // Verify it's an equality comparison
        assert!(matches!(inner.operator, crate::ast::InfixOperator::Equal(_)));

        // Left side should be identifier 'x'
        let Ast::Identifier(left_id) = inner.left.deref() else {
            panic!("Expected identifier on left side")
        };
        assert_eq!(left_id.value(), "x");

        // Right side should be number '5'
        let Literal(AstLiteral::Number(right_num)) = inner.right.deref() else {
            panic!("Expected number on right side")
        };
        assert_eq!(right_num.value(), "5");
    }
}
