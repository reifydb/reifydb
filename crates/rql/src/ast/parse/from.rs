// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::ast::AstFrom;
use crate::ast::lex::Operator::OpenParen;
use crate::ast::lex::{Keyword, Operator};
use crate::ast::parse;
use crate::ast::parse::Parser;

impl Parser {
    pub(crate) fn parse_from(&mut self) -> parse::Result<AstFrom> {
        let token = self.consume_keyword(Keyword::From)?;

        if self.current()?.is_operator(OpenParen) {
            Ok(AstFrom::Query { token, query: self.parse_block()? })
        } else {
            let identifier = self.parse_identifier()?;

            let (schema, table) = if !self.is_eof() && self.current()?.is_operator(Operator::Dot) {
                self.consume_operator(Operator::Dot)?;
                let table = self.parse_identifier()?;
                (Some(identifier), table)
            } else {
                (None, identifier)
            };

            Ok(AstFrom::Table { token, schema, table })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstFrom;
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;

    #[test]
    fn test_from_schema_and_table() {
        let tokens = lex("FROM reifydb.users").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let from = result.first_unchecked().as_from();

        match from {
            AstFrom::Table { table, schema, .. } => {
                assert_eq!(schema.as_ref().unwrap().value(), "reifydb");
                assert_eq!(table.value(), "users");
            }
            AstFrom::Query { .. } => unreachable!(),
        }
    }

    #[test]
    fn test_from_table_without_schema() {
        let tokens = lex("FROM users").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let from = result.first_unchecked().as_from();

        match from {
            AstFrom::Table { table, schema, .. } => {
                assert_eq!(schema, &None);
                assert_eq!(table.value(), "users");
            }
            AstFrom::Query { .. } => unreachable!(),
        }
    }

    #[test]
    fn test_from_block() {
        let tokens = lex("FROM ( FROM reifydb.users SELECT name )").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let from = result.first_unchecked().as_from();

        match from {
            AstFrom::Table { .. } => unreachable!(),
            AstFrom::Query { query, .. } => {
                let block = query;
                assert_eq!(block.len(), 2);

                let from = block[0].as_from();
                match from {
                    AstFrom::Table { table, schema, .. } => {
                        assert_eq!(schema.as_ref().unwrap().value(), "reifydb");
                        assert_eq!(table.value(), "users");
                    }
                    AstFrom::Query { .. } => unreachable!(),
                }

                let select = block[1].as_select();
                assert_eq!(select.columns.len(), 1);
                let column = select.columns[0].as_identifier();
                assert_eq!(column.value(), "name");
            }
        }
    }
}
