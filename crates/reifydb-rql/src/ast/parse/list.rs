// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Operator::CloseBracket;
use crate::ast::lex::{Operator, Separator};
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstList, TokenKind};

impl Parser {
    pub(crate) fn parse_list(&mut self) -> crate::Result<AstList> {
        let token = self.consume_operator(Operator::OpenBracket)?;

        let mut nodes = Vec::new();
        loop {
            self.skip_new_line()?;

            if self.current()?.is_operator(CloseBracket) {
                break;
            }

            self.consume_if(TokenKind::Separator(Separator::Comma))?;

            nodes.push(self.parse_node(Precedence::None)?);
        }

        self.consume_operator(CloseBracket)?;
        Ok(AstList { token, nodes })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;

    #[test]
    fn test_empty() {
        let tokens = lex("[]").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let list = result.first_unchecked().as_list();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_single() {
        let tokens = lex("[ 'ReifyDB' ]").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let list = result.first_unchecked().as_list();
        assert_eq!(list.len(), 1);

        let literal = list[0].as_literal_text();
        assert_eq!(literal.value(), "ReifyDB");
    }

    #[test]
    fn test_numbers() {
        let tokens = lex("[1, 2.2 , 2.34142]").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let list = result.first_unchecked().as_list();
        assert_eq!(list.len(), 3);

        let first = list[0].as_literal_number();
        assert_eq!(first.value(), "1");

        let second = list[1].as_literal_number();
        assert_eq!(second.value(), "2.2");

        let third = list[2].as_literal_number();
        assert_eq!(third.value(), "2.34142");
    }

    #[test]
    fn test_row() {
        let tokens = lex("[ { field: 'value' }]").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let list = result.first_unchecked().as_list();
        assert_eq!(list.len(), 1);

        let row = list[0].as_inline();
        assert_eq!(row.keyed_values.len(), 1);

        assert_eq!(row.keyed_values[0].key.value(), "field");
        assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value");
    }
}
