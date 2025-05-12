// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::{Keyword, Operator};
use crate::ast::parse::Parser;
use crate::ast::{AstInsert, parse};

impl Parser {
    pub(crate) fn parse_insert(&mut self) -> parse::Result<AstInsert> {
        let token = self.consume_keyword(Keyword::Insert)?;
        self.consume_keyword(Keyword::Into)?;

        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let store = self.parse_identifier()?;

        let columns = self.parse_tuple()?;

        self.consume_keyword(Keyword::Values)?;
        let values = self.parse_tuple()?;

        Ok(AstInsert::Values { token, schema, store, columns, values })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstInsert;
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;

    #[test]
    fn test_values() {
        let tokens = lex(r#"
        insert into test.users(id, name, is_premium) values (1, 'Alice', true)
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let insert = result.as_insert();

        match insert {
            AstInsert::Values { schema, store, columns, values, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(store.value(), "users");

                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].value(), "id");
                assert_eq!(columns[1].value(), "name");
                assert_eq!(columns[2].value(), "is_premium");

                assert_eq!(values.len(), 3);
                {
                    let id = values[0].as_literal_number();
                    assert_eq!(id.value(), "1");
                }
                {
                    let name = values[1].as_literal_text();
                    assert_eq!(name.value(), "Alice");
                }
                {
                    let is_premium = values[2].as_literal_boolean();
                    assert_eq!(is_premium.value(), true);
                }
            }
            _ => unreachable!(),
        }
    }
}
