// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstFilter, parse};

impl Parser {
    pub(crate) fn parse_filter(&mut self) -> parse::Result<AstFilter> {
        let token = self.consume_keyword(Keyword::Filter)?;
        let node = self.parse_node(Precedence::None)?;
        Ok(AstFilter { token, node: Box::new(node) })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::{Keyword, lex};
    use crate::ast::parse::Parser;
    use crate::ast::{Ast, InfixOperator, TokenKind};

    #[test]
    fn test_simple_comparison() {
        let tokens = lex("filter price > 100").unwrap();
        let mut parser = Parser::new(tokens);
        let filter = parser.parse_filter().unwrap();

        assert_eq!(filter.token.kind, TokenKind::Keyword(Keyword::Filter));

        let node = filter.node.as_infix();
        assert_eq!(node.left.as_identifier().name(), "price");
        assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
        assert_eq!(node.right.as_literal_number().value(), "100");
    }

    #[test]
    fn test_nested_expression() {
        let tokens = lex("filter (price + fee) > 100").unwrap();
        let mut parser = Parser::new(tokens);
        let filter = parser.parse_filter().unwrap();

        let node = filter.node.as_infix();
        assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
        assert_eq!(node.right.as_literal_number().value(), "100");

        let nested = node.left.as_tuple().nodes[0].as_infix();
        assert_eq!(nested.left.as_identifier().name(), "price");
        assert!(matches!(nested.operator, InfixOperator::Add(_)));
        assert_eq!(nested.right.as_identifier().name(), "fee");
    }

    #[test]
    fn test_filter_missing_expression() {
        let tokens = lex("filter").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_filter().unwrap();
        assert_eq!(*result.node, Ast::Nop);
    }
}
