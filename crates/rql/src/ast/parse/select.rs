// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword;
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{parse, AstSelect};

impl Parser {
    pub(crate) fn parse_select(&mut self) -> parse::Result<AstSelect> {
        let token = self.consume_keyword(Keyword::Select)?;

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

        Ok(AstSelect { token, columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;
    use crate::ast::Ast;

    #[test]
    fn test_select_star() {
        let tokens = lex("SELECT *").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let select = result.as_select();
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(select.columns[0], Ast::Wildcard(_)));
    }

    #[test]
    fn test_select_single_column() {
        let tokens = lex("SELECT name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let select = result.as_select();
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(select.columns[0], Ast::Identifier(_)));
        assert_eq!(select.columns[0].value(), "name");
    }

    #[test]
    fn test_select_multiple_columns() {
        let tokens = lex("SELECT name,age").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let select = result.as_select();
        assert_eq!(select.columns.len(), 2);
        assert!(matches!(select.columns[0], Ast::Identifier(_)));
        assert_eq!(select.columns[0].value(), "name");

        assert!(matches!(select.columns[1], Ast::Identifier(_)));
        assert_eq!(select.columns[1].value(), "age");
    }
}
