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
            Ok(AstFrom::Query { token, query: self.parse_tuple()? })
        } else {
            let schema = self.parse_identifier()?;
            self.consume_operator(Operator::Dot)?;
            let store = self.parse_identifier()?;
            Ok(AstFrom::Store { token, schema, store })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstFrom;
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;

    #[test]
    fn test_parse_from_identifier() {
        let tokens = lex("FROM reifydb.users").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let from = result.as_from();

        match from {
            AstFrom::Store { store, schema, .. } => {
                assert_eq!(store.value(), "users");
                assert_eq!(schema.value(), "reifydb");
            }
            AstFrom::Query { .. } => unreachable!(),
        }
    }

    #[test]
    fn test_parse_from_block() {
        let tokens = lex("FROM ( FROM reifydb.users SELECT name )").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let from = result.as_from();

        match from {
            AstFrom::Store { store, schema, .. } => unreachable!(),
            AstFrom::Query { query, .. } => {
                let tuple = query;
                assert_eq!(tuple.len(), 2);

                let from = tuple[0].as_from();
                match from {
                    AstFrom::Store { store, schema, .. } => {
                        assert_eq!(store.value(), "users");
                        assert_eq!(schema.value(), "reifydb");
                    }
                    AstFrom::Query { .. } => unreachable!(),
                }

                let select = tuple[1].as_select();
                assert_eq!(select.columns.len(), 1);
                let column = select.columns[0].as_identifier();
                assert_eq!(column.value(), "name");
            }
        }
    }
}
