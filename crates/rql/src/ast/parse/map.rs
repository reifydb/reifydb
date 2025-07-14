// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword;
use crate::ast::lex::Operator::{CloseCurly, OpenCurly};
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Error, Parser, Precedence};
use crate::ast::{AstMap, parse};
use reifydb_diagnostic::parse::multiple_expressions_without_braces;

impl Parser {
    pub(crate) fn parse_map(&mut self) -> parse::Result<AstMap> {
        let token = self.consume_keyword(Keyword::Map)?;

        // Check if we have an opening brace
        let has_braces = self.current()?.is_operator(OpenCurly);

        if has_braces {
            self.advance()?; // consume opening brace
        }

        let mut nodes = Vec::new();
        loop {
            nodes.push(self.parse_node(Precedence::None)?);

            if self.is_eof() {
                break;
            }

            // If we have braces, look for closing brace
            if has_braces && self.current()?.is_operator(CloseCurly) {
                self.advance()?; // consume closing brace
                break;
            }

            // consume comma and continue
            if self.current()?.is_separator(Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        if nodes.len() > 1 && !has_braces {
            return Err(Error::Passthrough {
                diagnostic: multiple_expressions_without_braces(token.span),
            });
        }

        Ok(AstMap { token, nodes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;
    use crate::ast::{Ast, AstInfix, InfixOperator};

    #[test]
    fn test_constant_number() {
        let tokens = lex("MAP 1").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 1);

        let number = map.nodes[0].as_literal_number();
        assert_eq!(number.value(), "1");
    }

    #[test]
    fn test_multiple_expressions() {
        let tokens = lex("MAP {1 + 2, 4 * 3}").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 2);

        let first = map.nodes[0].as_infix();
        assert_eq!(first.left.as_literal_number().value(), "1");
        assert!(matches!(first.operator, InfixOperator::Add(_)));
        assert_eq!(first.right.as_literal_number().value(), "2");

        let second = map.nodes[1].as_infix();
        assert_eq!(second.left.as_literal_number().value(), "4");
        assert!(matches!(second.operator, InfixOperator::Multiply(_)));
        assert_eq!(second.right.as_literal_number().value(), "3");
    }

    #[test]
    fn test_star() {
        let tokens = lex("MAP *").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 1);
        assert!(matches!(map.nodes[0], Ast::Wildcard(_)));
    }

    #[test]
    fn test_single_column() {
        let tokens = lex("MAP name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 1);
        assert!(matches!(map.nodes[0], Ast::Identifier(_)));
        assert_eq!(map.nodes[0].value(), "name");
    }

    #[test]
    fn test_multiple_columns() {
        let tokens = lex("MAP {name, age}").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 2);
        assert!(matches!(map.nodes[0], Ast::Identifier(_)));
        assert_eq!(map.nodes[0].value(), "name");

        assert!(matches!(map.nodes[1], Ast::Identifier(_)));
        assert_eq!(map.nodes[1].value(), "age");
    }

    #[test]
    fn test_as() {
        let tokens = lex("map 1 as a").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 1);

        let AstInfix { left, operator, right, .. } = map.nodes[0].as_infix();
        let left = left.as_literal_number();
        assert_eq!(left.value(), "1");

        assert!(matches!(operator, InfixOperator::As(_)));

        let right = right.as_identifier();
        assert_eq!(right.value(), "a");
    }

    #[test]
    fn test_single_expression_with_braces() {
        let tokens = lex("MAP {1}").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 1);

        let number = map.nodes[0].as_literal_number();
        assert_eq!(number.value(), "1");
    }

    #[test]
    fn test_multiple_expressions_without_braces_fails() {
        let tokens = lex("MAP 1, 2").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse();

        assert!(result.is_err(), "Expected error for multiple expressions without braces");
    }

    #[test]
    fn test_single_column_with_braces() {
        let tokens = lex("MAP {name}").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let map = result.first_unchecked().as_map();
        assert_eq!(map.nodes.len(), 1);
        assert!(matches!(map.nodes[0], Ast::Identifier(_)));
        assert_eq!(map.nodes[0].value(), "name");
    }
}
