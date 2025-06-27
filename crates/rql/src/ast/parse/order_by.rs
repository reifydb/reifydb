// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword;
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::Parser;
use crate::ast::{AstOrderBy, parse};

impl Parser {
    pub(crate) fn parse_order_by(&mut self) -> parse::Result<AstOrderBy> {
        let token = self.consume_keyword(Keyword::Order)?;
        let _ = self.consume_keyword(Keyword::By)?;

        let mut columns = Vec::new();
        let mut directions = Vec::new();

        loop {
            columns.push(self.parse_identifier()?);

            if !self.is_eof() && !self.current()?.is_separator(Comma) {
                directions.push(Some(self.parse_identifier()?));
            } else {
                directions.push(None);
            }

            if self.is_eof() {
                break;
            }

            if self.current()?.is_separator(Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        Ok(AstOrderBy { token, columns, directions })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;

    #[test]
    fn test_single_column() {
        let tokens = lex("ORDER BY name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let order_by = result.first_unchecked().as_order_by();
        assert_eq!(order_by.columns.len(), 1);
        assert_eq!(order_by.directions.len(), 1);

        assert_eq!(order_by.columns[0].value(), "name");
        assert_eq!(order_by.directions[0].as_ref(), None);
    }

    #[test]
    fn test_single_column_asc() {
        let tokens = lex("ORDER BY name ASC").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let order_by = result.first_unchecked().as_order_by();
        assert_eq!(order_by.columns.len(), 1);
        assert_eq!(order_by.directions.len(), 1);

        assert_eq!(order_by.columns[0].value(), "name");
        assert_eq!(order_by.directions[0].as_ref().unwrap().value(), "ASC");
    }

    #[test]
    fn test_single_column_desc() {
        let tokens = lex("ORDER BY name DESC").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let order_by = result.first_unchecked().as_order_by();
        assert_eq!(order_by.columns.len(), 1);
        assert_eq!(order_by.directions.len(), 1);

        assert_eq!(order_by.columns[0].value(), "name");
        assert_eq!(order_by.directions[0].as_ref().unwrap().value(), "DESC");
    }

    #[test]
    fn test_multiple_columns() {
        let tokens = lex("ORDER BY name,age").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let order_by = result.first_unchecked().as_order_by();
        assert_eq!(order_by.columns.len(), 2);
        assert_eq!(order_by.directions.len(), 2);

        assert_eq!(order_by.columns[0].value(), "name");
        assert_eq!(order_by.directions[0].as_ref(), None);

        assert_eq!(order_by.columns[1].value(), "age");
        assert_eq!(order_by.directions[1].as_ref(), None);
    }

    #[test]
    fn test_multiple_columns_asc_desc() {
        let tokens = lex("ORDER BY name ASC,age DESC").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let order_by = result.first_unchecked().as_order_by();
        assert_eq!(order_by.columns.len(), 2);
        assert_eq!(order_by.directions.len(), 2);

        assert_eq!(order_by.columns[0].value(), "name");
        assert_eq!(order_by.directions[0].as_ref().unwrap().value(), "ASC");

        assert_eq!(order_by.columns[1].value(), "age");
        assert_eq!(order_by.directions[1].as_ref().unwrap().value(), "DESC");
    }
}
