// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword;
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstOrderBy, parse};

impl Parser {
    pub(crate) fn parse_order_by(&mut self) -> parse::Result<AstOrderBy> {
        let token = self.consume_keyword(Keyword::Order)?;
        let _ = self.consume_keyword(Keyword::By)?;

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

        Ok(AstOrderBy { token, columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Ast;
    use crate::ast::lex::lex;

    #[test]
    fn test_single_column() {
        let tokens = lex("ORDER BY name").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let select = result.first_unchecked().as_order_by();
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(select.columns[0], Ast::Identifier(_)));
        assert_eq!(select.columns[0].value(), "name");
    }

    #[test]
    fn test_multiple_columns() {
        let tokens = lex("ORDER BY name,age").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();

        let result = result.pop().unwrap();
        let select = result.first_unchecked().as_order_by();
        assert_eq!(select.columns.len(), 2);
        assert!(matches!(select.columns[0], Ast::Identifier(_)));
        assert_eq!(select.columns[0].value(), "name");

        assert!(matches!(select.columns[1], Ast::Identifier(_)));
        assert_eq!(select.columns[1].value(), "age");
    }
}
