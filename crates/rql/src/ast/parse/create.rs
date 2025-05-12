// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword::Table;
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

        unimplemented!();
    }

    fn parse_schema(&mut self, token: Token) -> parse::Result<AstCreate> {
        Ok(AstCreate::Schema { token, name: self.parse_identifier()? })
    }

    fn parse_table(&mut self, token: Token) -> parse::Result<AstCreate> {
        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let name = self.parse_identifier()?;

        Ok(AstCreate::Table { token, name, schema })
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
    fn test_create_table() {
        let tokens = lex(r#"
            create table test.users(id: int2, name: text(255), is_premium: bool)
        "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let infix = result.as_infix();

        let left = infix.left.deref();
        let create = left.as_create();

        match create {
            AstCreate::Table { name, schema, .. } => {
                assert_eq!(schema.value(), "test");
                assert_eq!(name.value(), "users");
            }
            _ => unreachable!(),
        }

        assert!(matches!(infix.operator, InfixOperator::Call(_)));

        let right = infix.right.deref().as_tuple();
        assert_eq!(right.nodes.len(), 3);

        {
            let id = right.nodes[0].as_infix();
            let identifier = id.left.as_identifier();
            assert_eq!(identifier.value(), "id");

            assert!(matches!(id.operator, InfixOperator::TypeAscription(_)));

            let ty = id.right.as_type();
            assert_eq!(ty.value(), "int2")
        }

        {
            let name = right.nodes[1].as_infix();
            let left = name.left.as_infix();
            {
                let identifier = left.left.as_identifier();
                assert_eq!(identifier.value(), "name");

                assert!(matches!(left.operator, InfixOperator::TypeAscription(_)));

                let ty = left.right.as_type();
                assert_eq!(ty.value(), "text")
            }

            assert!(matches!(name.operator, InfixOperator::Call(_)));

            dbg!(&name.right);
            let tuple = name.right.deref().as_tuple();
            assert_eq!(tuple.nodes.len(), 1);

            let size = tuple.nodes[0].as_literal_number();
            assert_eq!(size.value(), "255");
        }

        {
            let is_premium = right.nodes[2].as_infix();
            let identifier = is_premium.left.as_identifier();
            assert_eq!(identifier.value(), "is_premium");

            assert!(matches!(is_premium.operator, InfixOperator::TypeAscription(_)));

            let ty = is_premium.right.as_type();
            assert_eq!(ty.value(), "bool")
        }
    }
}
