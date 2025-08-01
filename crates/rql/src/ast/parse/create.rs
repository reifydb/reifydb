// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword::{Computed, Series, Table, View, With};
use crate::ast::lex::Operator::CloseParen;
use crate::ast::lex::Separator::Comma;
use crate::ast::lex::{Keyword, Operator, Token, TokenKind};
use crate::ast::parse::Parser;
use crate::ast::{
    AstColumnToCreate, AstCreate, AstCreateComputedView, AstCreateSchema, AstCreateSeries,
    AstCreateTable,
};
use Keyword::{Create, Schema};
use Operator::Colon;

impl Parser {
    pub(crate) fn parse_create(&mut self) -> crate::Result<AstCreate> {
        let token = self.consume_keyword(Create)?;

        if (self.consume_if(TokenKind::Keyword(Schema))?).is_some() {
            return self.parse_schema(token);
        }

        if (self.consume_if(TokenKind::Keyword(Computed))?).is_some() {
            if (self.consume_if(TokenKind::Keyword(View))?).is_some() {
                return self.parse_computed_view(token);
            }
            unimplemented!()
        }

        if (self.consume_if(TokenKind::Keyword(Table))?).is_some() {
            return self.parse_table(token);
        }

        if (self.consume_if(TokenKind::Keyword(Series))?).is_some() {
            return self.parse_series(token);
        }

        unimplemented!();
    }

    fn parse_schema(&mut self, token: Token) -> crate::Result<AstCreate> {
        Ok(AstCreate::Schema(AstCreateSchema { token, name: self.parse_identifier()? }))
    }

    fn parse_series(&mut self, token: Token) -> crate::Result<AstCreate> {
        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let name = self.parse_identifier()?;
        let columns = self.parse_columns()?;

        Ok(AstCreate::Series(AstCreateSeries { token, name, schema, columns }))
    }

    fn parse_computed_view(&mut self, token: Token) -> crate::Result<AstCreate> {
        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let name = self.parse_identifier()?;
        let columns = self.parse_columns()?;

        // Parse optional WITH clause
        let with = if self.consume_if(TokenKind::Keyword(With))?.is_some() {
            // Expect opening curly brace
            self.consume_operator(Operator::OpenCurly)?;

            // Parse the query nodes inside the WITH clause
            let mut query_nodes = Vec::new();

            // Parse statements until we hit the closing brace
            loop {
                if self.is_eof()
                    || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly)
                {
                    break;
                }

                let node = self.parse_node(crate::ast::parse::Precedence::None)?;
                query_nodes.push(node);
            }

            // Expect closing curly brace
            self.consume_operator(Operator::CloseCurly)?;

            Some(crate::ast::AstStatement(query_nodes))
        } else {
            None
        };

        Ok(AstCreate::ComputedView(AstCreateComputedView {
            token,
            view: name,
            schema,
            columns,
            with,
        }))
    }

    fn parse_table(&mut self, token: Token) -> crate::Result<AstCreate> {
        let schema = self.parse_identifier()?;
        self.consume_operator(Operator::Dot)?;
        let name = self.parse_identifier()?;
        let columns = self.parse_columns()?;

        Ok(AstCreate::Table(AstCreateTable { token, table: name, schema, columns }))
    }

    fn parse_columns(&mut self) -> crate::Result<Vec<AstColumnToCreate>> {
        let mut result = Vec::new();

        self.consume_operator(Operator::OpenParen)?;
        loop {
            self.skip_new_line()?;

            if self.current()?.is_operator(CloseParen) {
                break;
            }
            result.push(self.parse_column()?);
            if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
                break;
            };
        }
        self.consume_operator(CloseParen)?;
        Ok(result)
    }

    fn parse_column(&mut self) -> crate::Result<AstColumnToCreate> {
        let name = self.parse_identifier()?;
        self.consume_operator(Colon)?;
        let ty = self.parse_identifier()?;

        let policies = if self.current()?.is_keyword(Keyword::Policy) {
            Some(self.parse_policy_block()?)
        } else {
            None
        };

        Ok(AstColumnToCreate { name, ty, policies })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;
    use crate::ast::{
        AstCreate, AstCreateComputedView, AstCreateSchema, AstCreateSeries, AstCreateTable,
        AstPolicyKind,
    };

    #[test]
    fn test_create_schema() {
        let tokens = lex("CREATE SCHEMA REIFYDB").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Schema(AstCreateSchema { name, .. }) => {
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
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Series(AstCreateSeries { name, schema, columns, .. }) => {
                assert_eq!(schema.value(), "test");
                assert_eq!(name.value(), "metrics");

                assert_eq!(columns.len(), 1);

                assert_eq!(columns[0].name.value(), "value");
                assert_eq!(columns[0].ty.value(), "Int2");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_table() {
        let tokens = lex(r#"
        create table test.users(id: int2, name: text, is_premium: bool)
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Table(AstCreateTable { table: name, schema, columns, .. }) => {
                assert_eq!(schema.value(), "test");
                assert_eq!(name.value(), "users");
                assert_eq!(columns.len(), 3);

                {
                    let col = &columns[0];
                    assert_eq!(col.name.value(), "id");
                    assert_eq!(col.ty.value(), "int2");
                    assert!(col.policies.is_none());
                }

                {
                    let col = &columns[1];
                    assert_eq!(col.name.value(), "name");
                    assert_eq!(col.ty.value(), "text");
                }

                {
                    let col = &columns[2];
                    assert_eq!(col.name.value(), "is_premium");
                    assert_eq!(col.ty.value(), "bool");
                    assert!(col.policies.is_none());
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_table_with_saturation_policy() {
        let tokens = lex(r#"
        create table test.items(field: int2 policy (saturation error) )
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();

        match create {
            AstCreate::Table(AstCreateTable { table: name, schema, columns, .. }) => {
                assert_eq!(schema.value(), "test");
                assert_eq!(name.value(), "items");

                assert_eq!(columns.len(), 1);

                let col = &columns[0];
                assert_eq!(col.name.value(), "field");
                assert_eq!(col.ty.value(), "int2");

                let policies = &col.policies.as_ref().unwrap().policies;
                assert_eq!(policies.len(), 1);
                let policy = &policies[0];
                assert!(matches!(policy.policy, AstPolicyKind::Saturation));
                assert_eq!(policy.value.as_identifier().value(), "error");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_create_computed_view() {
        let tokens = lex(r#"
        create computed view test.views(field: int2 policy (saturation error))
    "#)
        .unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let create = result.first_unchecked().as_create();
        match create {
            AstCreate::ComputedView(AstCreateComputedView {
                view: name, schema, columns, ..
            }) => {
                assert_eq!(schema.value(), "test");
                assert_eq!(name.value(), "views");

                assert_eq!(columns.len(), 1);

                let col = &columns[0];
                assert_eq!(col.name.value(), "field");
                assert_eq!(col.ty.value(), "int2");

                let policies = &col.policies.as_ref().unwrap().policies;
                assert_eq!(policies.len(), 1);
                let policy = &policies[0];
                assert!(matches!(policy.policy, AstPolicyKind::Saturation));
                assert_eq!(policy.value.as_identifier().value(), "error");
            }
            _ => unreachable!(),
        }
    }
}
