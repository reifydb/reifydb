// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword::{Index, Unique, Primary, Key, Asc, Desc, Filter, Map, On};
use crate::ast::lex::Separator::Comma;
use crate::ast::lex::{Operator, Token, TokenKind};
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstCreate, AstCreateIndex, AstIndexColumn};
use reifydb_core::SortDirection;

impl Parser {
    pub(crate) fn peek_is_index_creation(&mut self) -> crate::Result<bool> {
        Ok(matches!(
            self.current()?.kind,
            TokenKind::Keyword(Index) | TokenKind::Keyword(Unique) | TokenKind::Keyword(Primary)
        ))
    }

    pub(crate) fn parse_create_index(&mut self, create_token: Token) -> crate::Result<AstCreate> {
        let index_type = self.parse_index_type()?;
        
        let name = if self.current()?.is_keyword(On) {
            None
        } else {
            Some(self.parse_identifier()?)
        };
        
        self.consume_keyword(On)?;
        
        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let table = self.parse_identifier()?;
        
        let columns = self.parse_index_columns()?;
        
        let mut filters = Vec::new();
        while self.consume_if(TokenKind::Keyword(Filter))?.is_some() {
            filters.push(Box::new(self.parse_node(Precedence::None)?));
        }
        
        let map = if self.consume_if(TokenKind::Keyword(Map))?.is_some() {
            Some(Box::new(self.parse_node(Precedence::None)?))
        } else {
            None
        };
        
        Ok(AstCreate::Index(AstCreateIndex {
            token: create_token,
            index_type,
            name,
            schema,
            table,
            columns,
            filters,
            map,
        }))
    }

    fn parse_index_type(&mut self) -> crate::Result<reifydb_core::IndexType> {
        if self.consume_if(TokenKind::Keyword(Primary))?.is_some() {
            self.consume_keyword(Key)?;
            Ok(reifydb_core::IndexType::Primary)
        } else if self.consume_if(TokenKind::Keyword(Unique))?.is_some() {
            self.consume_keyword(Index)?;
            Ok(reifydb_core::IndexType::Unique)
        } else {
            self.consume_keyword(Index)?;
            Ok(reifydb_core::IndexType::Index)
        }
    }

    fn parse_index_columns(&mut self) -> crate::Result<Vec<AstIndexColumn>> {
        let mut columns = Vec::new();
        
        self.consume_operator(Operator::OpenCurly)?;
        
        loop {
            self.skip_new_line()?;
            
            if self.current()?.is_operator(Operator::CloseCurly) {
                break;
            }
            
            let column = self.parse_identifier()?;
            
            let order = if self.consume_if(TokenKind::Keyword(Asc))?.is_some() {
                Some(SortDirection::Asc)
            } else if self.consume_if(TokenKind::Keyword(Desc))?.is_some() {
                Some(SortDirection::Desc)
            } else {
                None
            };
            
            columns.push(AstIndexColumn { column, order });
            
            if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
                break;
            }
        }
        
        self.consume_operator(Operator::CloseCurly)?;
        
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;
    use crate::ast::{AstCreate, AstCreateIndex};
    use reifydb_core::SortDirection;

    #[test]
    fn test_create_index() {
        let tokens = lex(r#"create index on test.users {email}"#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { index_type, name, schema, table, columns, filters, .. }) => {
                assert_eq!(*index_type, reifydb_core::IndexType::Index);
                assert!(name.is_none());
                assert_eq!(schema.value(), "test");
                assert_eq!(table.value(), "users");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].column.value(), "email");
                assert!(columns[0].order.is_none());
                assert_eq!(filters.len(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_unique_index() {
        let tokens = lex(r#"create unique index idx_email on test.users {email}"#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { index_type, name, schema, table, columns, filters, .. }) => {
                assert_eq!(*index_type, reifydb_core::IndexType::Unique);
                assert_eq!(name.as_ref().unwrap().value(), "idx_email");
                assert_eq!(schema.value(), "test");
                assert_eq!(table.value(), "users");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].column.value(), "email");
                assert_eq!(filters.len(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_primary_key() {
        let tokens = lex(r#"create primary key on test.users {id}"#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { index_type, name, schema, table, columns, filters, .. }) => {
                assert_eq!(*index_type, reifydb_core::IndexType::Primary);
                assert!(name.is_none());
                assert_eq!(schema.value(), "test");
                assert_eq!(table.value(), "users");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].column.value(), "id");
                assert_eq!(filters.len(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_composite_index() {
        let tokens = lex(r#"create index on test.users {last_name, first_name}"#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { columns, filters, .. }) => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].column.value(), "last_name");
                assert_eq!(columns[1].column.value(), "first_name");
                assert_eq!(filters.len(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_index_with_ordering() {
        let tokens = lex(r#"create index on test.users {created_at desc, status asc}"#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { columns, filters, .. }) => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].column.value(), "created_at");
                assert_eq!(columns[0].order, Some(SortDirection::Desc));
                assert_eq!(columns[1].column.value(), "status");
                assert_eq!(columns[1].order, Some(SortDirection::Asc));
                assert_eq!(filters.len(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_index_with_single_filter() {
        let tokens = lex(r#"create index on test.users {email} filter active == true"#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { columns, filters, .. }) => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].column.value(), "email");
                assert_eq!(filters.len(), 1);
                // Verify filter contains a comparison expression
                assert!(filters[0].is_infix());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_index_with_multiple_filters() {
        let tokens = lex(r#"create index on test.users {email} filter active == true filter age > 18 filter country == "US""#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { columns, filters, .. }) => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].column.value(), "email");
                assert_eq!(filters.len(), 3);
                // Verify each filter is an infix expression
                assert!(filters[0].is_infix());
                assert!(filters[1].is_infix());
                assert!(filters[2].is_infix());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_index_with_filters_and_map() {
        let tokens = lex(r#"create index on test.users {email} filter active == true filter age > 18 map email"#).unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Index(AstCreateIndex { columns, filters, map, .. }) => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].column.value(), "email");
                assert_eq!(filters.len(), 2);
                assert!(filters[0].is_infix());
                assert!(filters[1].is_infix());
                assert!(map.is_some());
            }
            _ => unreachable!(),
        }
    }
}