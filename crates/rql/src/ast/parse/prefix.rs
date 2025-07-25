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
        let expr = self.parse_node(Precedence::Prefix)?;

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
}
