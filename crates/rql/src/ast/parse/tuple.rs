// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Operator::CloseParen;
use crate::ast::lex::{Operator, Separator, Token, TokenKind};
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstTuple, parse};
use Separator::Comma;

impl Parser {
    pub(crate) fn parse_tuple(&mut self) -> parse::Result<AstTuple> {
        let token = self.consume_operator(Operator::OpenParen)?;
        self.parse_tuple_call(token)
    }

    pub(crate) fn parse_tuple_call(&mut self, operator: Token) -> parse::Result<AstTuple> {
        let mut nodes = Vec::new();
        loop {
            self.skip_new_line()?;

            if self.current()?.is_operator(CloseParen) {
                break;
            }
            nodes.push(self.parse_node(Precedence::None)?);
            if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
                break;
            };
        }

        self.consume_operator(CloseParen)?;
        Ok(AstTuple { token: operator, nodes })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Ast::{Identifier, Infix, Literal};
    use crate::ast::AstLiteral::Number;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::ast::{AstInfix, InfixOperator};

    #[test]
    fn test_empty_tuple() {
        let tokens = lex("()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();
        assert_eq!(node.nodes, vec![]);
    }

    #[test]
    fn test_tuple_with_number() {
        let tokens = lex("(9924)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Literal(Number(number)) = &node else { panic!() };
        assert_eq!(number.value(), "9924");
    }

    #[test]
    fn test_nested_tuple() {
        let tokens = lex("(1 * ( 2 + 3 ))").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Infix(AstInfix { left, right, .. }) = &node else { panic!() };

        let Literal(Number(left)) = &left.as_ref() else { panic!() };
        assert_eq!(left.value(), "1");

        let node = right.as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let AstInfix { left, right, .. } = &node.as_infix();

        let Literal(Number(left)) = &left.as_ref() else { panic!() };
        assert_eq!(left.value(), "2");

        let Literal(Number(right)) = &right.as_ref() else { panic!() };
        assert_eq!(right.value(), "3");
    }

    #[test]
    fn test_tuple_with_identifier() {
        let tokens = lex("(u)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = &result[0].first_unchecked().as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Identifier(node) = node else { panic!() };
        assert_eq!(node.value(), "u");
    }

    #[test]
    fn test_tuple_with_identifier_and_type() {
        let tokens = lex("(u: Bool)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();
        let Some(node) = node.nodes.first() else { panic!() };
        let Infix(AstInfix { left, right, .. }) = &node else { panic!() };

        let identifier = &left.as_identifier();
        assert_eq!(identifier.value(), "u");

        let dat_type = &right.as_identifier();
        assert_eq!(dat_type.value(), "Bool");
    }

    #[test]
    fn test_tuple_with_multiple_identifiers() {
        let tokens = lex("(u,v)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();

        let Some(Identifier(u_node)) = &node.nodes.first() else { panic!() };
        assert_eq!(u_node.value(), "u");

        let Some(Identifier(v_node)) = &node.nodes.last() else { panic!() };
        assert_eq!(v_node.value(), "v");
    }

    #[test]
    fn test_tuple_with_identifiers_and_types() {
        let tokens = lex("(u: Bool, v: Text)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();

        let Some(u_node) = node.nodes.first() else { panic!() };
        let Infix(AstInfix { left, right, .. }) = &u_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "u");
        let Identifier(ty) = &right.as_ref() else { panic!() };
        assert_eq!(ty.value(), "Bool");

        let Some(v_node) = node.nodes.last() else { panic!() };
        let Infix(AstInfix { left, right, .. }) = &v_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "v");
        let Identifier(identifier) = &right.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "Text");
    }

    #[test]
    fn test_tuple_with_identifiers_and_declaration() {
        let tokens = lex("(u = 1, v = 2)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();

        let Some(u_node) = node.nodes.first() else { panic!() };
        let Infix(AstInfix { left, operator, right, .. }) = &u_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "u");
        assert!(matches!(operator, InfixOperator::Assign(_)));
        let Literal(Number(number)) = right.as_ref() else { panic!() };
        assert_eq!(number.value(), "1");

        let Some(v_node) = node.nodes.last() else { panic!() };
        let Infix(AstInfix { left, operator, right, .. }) = &v_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "v");
        assert!(matches!(operator, InfixOperator::Assign(_)));
        let Literal(Number(number)) = right.as_ref() else { panic!() };
        assert_eq!(number.value(), "2");
    }

    #[test]
    fn test_multiline_tuple() {
        let tokens = lex(r#"(
        u: Bool,
        v: Text
        )"#)
        .unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();

        let Some(u_node) = node.nodes.first() else { panic!() };
        let Infix(AstInfix { left, right, .. }) = &u_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "u");
        let Identifier(identifier) = &right.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "Bool");

        let Some(v_node) = node.nodes.last() else { panic!() };
        let Infix(AstInfix { left, right, .. }) = &v_node else { panic!() };
        let Identifier(identifier) = &left.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "v");
        let Identifier(identifier) = &right.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "Text");
    }

    #[test]
    fn test_regression() {
        let tokens = lex("(-1 -2)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let node = result[0].first_unchecked().as_tuple();
        assert_eq!(node.nodes.len(), 1);

        let infix = node.nodes[0].as_infix();

        let left_number = infix.left.as_literal_number();
        assert_eq!(left_number.value(), "-1");

        assert!(matches!(infix.operator, InfixOperator::Subtract(_)));

        let right_number = infix.right.as_literal_number();
        assert_eq!(right_number.value(), "2");
    }
}
