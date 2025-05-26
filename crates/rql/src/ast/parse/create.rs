// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword::{Series, Table};
use crate::ast::lex::{Keyword, Operator, Token, TokenKind};
use crate::ast::parse::Parser;
use crate::ast::{AstCreate, parse};
use Keyword::{Create, Schema};

impl Parser {
    pub(crate) fn parse_create(&mut self) -> parse::Result<AstCreate> {
        let token = self.consume_keyword(Create)?;

        if let Some(_) = self.consume_if(TokenKind::Keyword(Schema))? {
            return self.parse_schema(token);
        }

        if let Some(_) = self.consume_if(TokenKind::Keyword(Table))? {
            return self.parse_table(token);
        }

        if let Some(_) = self.consume_if(TokenKind::Keyword(Series))? {
            return self.parse_series(token);
        }

        unimplemented!();
    }

    fn parse_schema(&mut self, token: Token) -> parse::Result<AstCreate> {
        Ok(AstCreate::Schema { token, name: self.parse_identifier()? })
    }

    fn parse_series(&mut self, token: Token) -> parse::Result<AstCreate> {
        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let name = self.parse_identifier()?;
        let definition = self.parse_tuple()?;

        Ok(AstCreate::Series { token, name, schema, definitions: definition })
    }

    fn parse_table(&mut self, token: Token) -> parse::Result<AstCreate> {
        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let name = self.parse_identifier()?;
        let definition = self.parse_tuple()?;

        Ok(AstCreate::Table { token, name, schema, definitions: definition })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;
    use crate::ast::{AstCreate, InfixOperator};
    use std::ops::Deref;

    #[test]
    fn test_create_schema() {
        let tokens = lex("CREATE SCHEMA REIFYDB").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.as_create();

        match create {
            AstCreate::Schema { name, .. } => {
                assert_eq!(name.value(), "REIFYDB");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_series() {
        let tokens = lex(r#"
            create series test.metrics(value: Int2)
        "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.as_create();

        match create {
            AstCreate::Series { name, schema, definitions, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(name.value(), "metrics");

                assert_eq!(definitions.nodes.len(), 1);

                {
                    let id = definitions.nodes[0].as_infix();
                    let identifier = id.left.as_identifier();
                    assert_eq!(identifier.value(), "value");

                    assert!(matches!(id.operator, InfixOperator::TypeAscription(_)));

                    let ty = id.right.as_type();
                    assert_eq!(ty.value(), "Int2")
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_table() {
        let tokens = lex(r#"
            create table test.users(id: int2, name: text(255), is_premium: bool)
        "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.as_create();

        match create {
            AstCreate::Table { name, schema, definitions, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(name.value(), "users");

                assert_eq!(definitions.nodes.len(), 3);

                {
                    let id = definitions.nodes[0].as_infix();
                    let identifier = id.left.as_identifier();
                    assert_eq!(identifier.value(), "id");

                    assert!(matches!(id.operator, InfixOperator::TypeAscription(_)));

                    let ty = id.right.as_type();
                    assert_eq!(ty.value(), "int2")
                }

                {
                    let name = definitions.nodes[1].as_infix();
                    let left = name.left.as_infix();
                    {
                        let identifier = left.left.as_identifier();
                        assert_eq!(identifier.value(), "name");

                        assert!(matches!(left.operator, InfixOperator::TypeAscription(_)));

                        let ty = left.right.as_type();
                        assert_eq!(ty.value(), "text")
                    }

                    assert!(matches!(name.operator, InfixOperator::Call(_)));

                    let tuple = name.right.deref().as_tuple();
                    assert_eq!(tuple.nodes.len(), 1);

                    let size = tuple.nodes[0].as_literal_number();
                    assert_eq!(size.value(), "255");
                }

                {
                    let is_premium = definitions.nodes[2].as_infix();
                    let identifier = is_premium.left.as_identifier();
                    assert_eq!(identifier.value(), "is_premium");

                    assert!(matches!(is_premium.operator, InfixOperator::TypeAscription(_)));

                    let ty = is_premium.right.as_type();
                    assert_eq!(ty.value(), "bool")
                }
            }
            _ => unreachable!(),
        }
    }
}
