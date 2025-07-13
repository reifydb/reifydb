// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword;
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstMap, parse};

impl Parser {
    pub(crate) fn parse_map(&mut self) -> parse::Result<AstMap> {
        let token = self.consume_keyword(Keyword::Map)?;

        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_node(Precedence::None)?);

            if self.is_eof() {
                break;
            }

            // consume comma and continue
            if self.current()?.is_separator(Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        Ok(AstMap { token, map: columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;
    use crate::ast::{Ast, AstInfix, InfixOperator};

    #[test]
    fn test_map_constant_number() {
        let tokens = lex("MAP 1").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.map.len(), 1);

        let number = map.map[0].as_literal_number();
        assert_eq!(number.value(), "1");
    }

    #[test]
    fn test_map_multiple_expressions() {
        let tokens = lex("MAP 1 + 2, 4 * 3").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.map.len(), 2);

        let first = map.map[0].as_infix();
        assert_eq!(first.left.as_literal_number().value(), "1");
        assert!(matches!(first.operator, InfixOperator::Add(_)));
        assert_eq!(first.right.as_literal_number().value(), "2");

        let second = map.map[1].as_infix();
        assert_eq!(second.left.as_literal_number().value(), "4");
        assert!(matches!(second.operator, InfixOperator::Multiply(_)));
        assert_eq!(second.right.as_literal_number().value(), "3");
    }

    #[test]
    fn test_map_star() {
        let tokens = lex("MAP *").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.map.len(), 1);
        assert!(matches!(map.map[0], Ast::Wildcard(_)));
    }

    #[test]
    fn test_map_single_column() {
        let tokens = lex("MAP name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.map.len(), 1);
        assert!(matches!(map.map[0], Ast::Identifier(_)));
        assert_eq!(map.map[0].value(), "name");
    }

    #[test]
    fn test_map_multiple_columns() {
        let tokens = lex("MAP name,age").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.map.len(), 2);
        assert!(matches!(map.map[0], Ast::Identifier(_)));
        assert_eq!(map.map[0].value(), "name");

        assert!(matches!(map.map[1], Ast::Identifier(_)));
        assert_eq!(map.map[1].value(), "age");
    }

    #[test]
    fn test_map_as() {
        let tokens = lex("map 1 as a").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.map.len(), 1);

        let AstInfix { left, operator, right, .. } = map.map[0].as_infix();
        let left = left.as_literal_number();
        assert_eq!(left.value(), "1");

        assert!(matches!(operator, InfixOperator::As(_)));

        let right = right.as_identifier();
        assert_eq!(right.value(), "a");
    }
}
