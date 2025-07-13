// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword;
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstAggregate, parse};

impl Parser {
    pub(crate) fn parse_group_by(&mut self) -> parse::Result<AstAggregate> {
        let token = self.consume_keyword(Keyword::Aggregate)?;

        let mut projections = Vec::new();
        loop {
            if self.current()?.is_keyword(Keyword::By) {
                break;
            }

            projections.push(self.parse_node(Precedence::None)?);

            if self.is_eof() {
                break;
            }

            if self.current()?.is_separator(Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        let _ = self.consume_keyword(Keyword::By)?;

        let mut by = Vec::new();

        loop {
            by.push(self.parse_node(Precedence::None)?);

            if self.is_eof() {
                break;
            }

            if self.current()?.is_separator(Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        Ok(AstAggregate { token, by, map: projections })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;
    use crate::ast::{Ast, InfixOperator};

    #[test]
    fn test_single_column() {
        let tokens = lex("AGGREGATE min(age) BY name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let aggregate = result.first_unchecked().as_aggregate_by();
        assert_eq!(aggregate.map.len(), 1);

        let projection = &aggregate.map[0].as_infix();
        let identifier = projection.left.as_identifier();
        assert_eq!(identifier.value(), "min");

        assert!(matches!(projection.operator, InfixOperator::Call(_)));
        let tuple = projection.right.as_tuple();
        let identifier = tuple.nodes[0].as_identifier();
        assert_eq!(identifier.value(), "age");

        assert_eq!(aggregate.by.len(), 1);
        assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
        assert_eq!(aggregate.by[0].value(), "name");
    }

    #[test]
    fn test_alias() {
        let tokens = lex("AGGREGATE min(age) as min_age BY name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let aggregate = result.first_unchecked().as_aggregate_by();
        assert_eq!(aggregate.map.len(), 1);

        let projection = &aggregate.map[0].as_infix();
        
        let min_age = projection.left.as_infix();
        let identifier = min_age.left.as_identifier();
        assert_eq!(identifier.value(), "min");

        assert!(matches!(min_age.operator, InfixOperator::Call(_)));
        let tuple = min_age.right.as_tuple();
        let identifier = tuple.nodes[0].as_identifier();
        assert_eq!(identifier.value(), "age");
        
        assert!(matches!(projection.operator, InfixOperator::As(_)));
        let identifier = projection.right.as_identifier();
        assert_eq!(identifier.value(), "min_age");

        assert_eq!(aggregate.by.len(), 1);
        assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
        assert_eq!(aggregate.by[0].value(), "name");
    }

    #[test]
    fn test_no_projection_single_column() {
        let tokens = lex("AGGREGATE BY name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let aggregate = result.first_unchecked().as_aggregate_by();
        assert_eq!(aggregate.map.len(), 0);

        assert_eq!(aggregate.by.len(), 1);
        assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
        assert_eq!(aggregate.by[0].value(), "name");
    }

    #[test]
    fn test_no_projection_multiple_columns() {
        let tokens = lex("AGGREGATE BY name,age").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let aggregate = result.first_unchecked().as_aggregate_by();
        assert_eq!(aggregate.map.len(), 0);
        assert_eq!(aggregate.by.len(), 2);

        assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
        assert_eq!(aggregate.by[0].value(), "name");

        assert!(matches!(aggregate.by[1], Ast::Identifier(_)));
        assert_eq!(aggregate.by[1].value(), "age");
    }

    #[test]
    fn test_many() {
        let tokens = lex("AGGREGATE min(age), max(age) BY name, gender").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let aggregate = result.first_unchecked().as_aggregate_by();
        assert_eq!(aggregate.map.len(), 2);

        let projection = &aggregate.map[0].as_infix();
        let identifier = projection.left.as_identifier();
        assert_eq!(identifier.value(), "min");

        assert!(matches!(projection.operator, InfixOperator::Call(_)));
        let tuple = projection.right.as_tuple();
        let identifier = tuple.nodes[0].as_identifier();
        assert_eq!(identifier.value(), "age");

        let projection = &aggregate.map[1].as_infix();
        let identifier = projection.left.as_identifier();
        assert_eq!(identifier.value(), "max");

        assert!(matches!(projection.operator, InfixOperator::Call(_)));
        let tuple = projection.right.as_tuple();
        let identifier = tuple.nodes[0].as_identifier();
        assert_eq!(identifier.value(), "age");

        assert_eq!(aggregate.by.len(), 2);
        assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
        assert_eq!(aggregate.by[0].value(), "name");

        assert!(matches!(aggregate.by[1], Ast::Identifier(_)));
        assert_eq!(aggregate.by[1].value(), "gender");
    }
}
