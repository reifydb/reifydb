// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword::Into;
use crate::ast::lex::Operator::OpenParen;
use crate::ast::lex::{Keyword, Operator, Separator, TokenKind};
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstInsertIntoTable, AstTuple, parse};

impl Parser {
    pub(crate) fn parse_insert(&mut self) -> parse::Result<AstInsertIntoTable> {
        let token = self.consume_keyword(Keyword::Insert)?;

        let mut rows = Vec::new();

        if self.current()?.kind == TokenKind::Operator(OpenParen) {
            loop {
                let tuple = self.parse_tuple()?;
                rows.push(tuple);
                if self.consume_if(TokenKind::Keyword(Into))?.is_some() {
                    break;
                }
                self.consume_separator(Separator::Comma)?;
            }
        } else {
            let mut nodes = Vec::new();
            loop {
                let ast = self.parse_node(Precedence::None)?;
                nodes.push(ast);
                if self.consume_if(TokenKind::Keyword(Into))?.is_some() {
                    break;
                }
                self.consume_separator(Separator::Comma)?;
            }
            rows.push(AstTuple { token: nodes[0].token().clone(), nodes });
        }

        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let store = self.parse_identifier()?;

        let columns = self.parse_tuple()?;

        Ok(AstInsertIntoTable { token, schema, table: store, columns, rows })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstInsertIntoTable;
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;

    #[test]
    fn test_single_row() {
        let tokens = lex(r#"
        insert (1, 'Alice', true) into test.users(id, name, is_premium)
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let insert = result.first_unchecked().as_insert();

        match insert {
            AstInsertIntoTable { schema, table: store, columns, rows, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(store.value(), "users");

                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].value(), "id");
                assert_eq!(columns[1].value(), "name");
                assert_eq!(columns[2].value(), "is_premium");

                assert_eq!(rows.len(), 1);
                let row = &rows[0];

                assert_eq!(row.len(), 3);
                {
                    let id = row[0].as_literal_number();
                    assert_eq!(id.value(), "1");
                }
                {
                    let name = row[1].as_literal_text();
                    assert_eq!(name.value(), "Alice");
                }
                {
                    let is_premium = row[2].as_literal_boolean();
                    assert!(is_premium.value());
                }
            }
        }
    }

    #[test]
    fn test_without_paren() {
        let tokens = lex(r#"
        insert
            1, 'Alice', true
        into test.users(id, name, is_premium)
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let insert = result.first_unchecked().as_insert();

        match insert {
            AstInsertIntoTable { schema, table: store, columns, rows, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(store.value(), "users");

                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].value(), "id");
                assert_eq!(columns[1].value(), "name");
                assert_eq!(columns[2].value(), "is_premium");

                assert_eq!(rows.len(), 1);
                let row = &rows[0];

                assert_eq!(row.len(), 3);
                {
                    let id = row[0].as_literal_number();
                    assert_eq!(id.value(), "1");
                }
                {
                    let name = row[1].as_literal_text();
                    assert_eq!(name.value(), "Alice");
                }
                {
                    let is_premium = row[2].as_literal_boolean();
                    assert!(is_premium.value());
                }
            }
        }
    }

    #[test]
    fn test_multiple_rows() {
        let tokens = lex(r#"
        insert
            (1, 'Alice', true),
            (2, 'Bob', false)
        into test.users(id, name, is_premium)
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let insert = result.first_unchecked().as_insert();

        match insert {
            AstInsertIntoTable { schema, table: store, columns, rows, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(store.value(), "users");

                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].value(), "id");
                assert_eq!(columns[1].value(), "name");
                assert_eq!(columns[2].value(), "is_premium");

                assert_eq!(rows.len(), 2);
                let row = &rows[0];

                assert_eq!(row.len(), 3);
                {
                    let id = row[0].as_literal_number();
                    assert_eq!(id.value(), "1");
                }
                {
                    let name = row[1].as_literal_text();
                    assert_eq!(name.value(), "Alice");
                }
                {
                    let is_premium = row[2].as_literal_boolean();
                    assert!(is_premium.value());
                }
            }
        }
    }

    #[test]
    fn test_sub_query() {
        let tokens = lex(r#"
        insert (map 1, 'Eve', false)
        into test.users (id, name, is_premium)
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let insert = result.first_unchecked().as_insert();

        match insert {
            AstInsertIntoTable { schema, table: store, columns, rows, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(store.value(), "users");

                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].value(), "id");
                assert_eq!(columns[1].value(), "name");
                assert_eq!(columns[2].value(), "is_premium");

                assert_eq!(rows.len(), 1);
                let row = &rows[0];
                assert_eq!(row.len(), 1);

                let map = row[0].as_map();
                assert_eq!(map.len(), 3);

                {
                    let id = map[0].as_literal_number();
                    assert_eq!(id.value(), "1");
                }
                {
                    let name = map[1].as_literal_text();
                    assert_eq!(name.value(), "Eve");
                }
                {
                    let is_premium = map[2].as_literal_boolean();
                    assert!(!is_premium.value());
                }
            }
        }
    }
}
