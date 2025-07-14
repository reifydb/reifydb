// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::ast::lex::Operator;
use crate::ast::lex::Operator::{CloseCurly, Colon};
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstRow, AstRowField, TokenKind, parse};

impl Parser {
    pub(crate) fn parse_row(&mut self) -> parse::Result<AstRow> {
        let token = self.consume_operator(Operator::OpenCurly)?;

        let mut fields = Vec::new();
        loop {
            self.skip_new_line()?;

            if self.current()?.is_operator(CloseCurly) {
                break;
            }

            let key = self.parse_identifier()?;
            self.consume_operator(Colon)?;
            let value = Box::new(self.parse_node(Precedence::None)?);

            fields.push(AstRowField { key, value });

            self.skip_new_line()?;
            self.consume_if(TokenKind::Separator(Comma))?;
        }

        self.consume_operator(CloseCurly)?;
        Ok(AstRow { token, fields })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Ast::{Identifier, Literal};
    use crate::ast::AstLiteral::{Number, Text};
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;

    #[test]
    fn test_empty_row() {
        let tokens = lex("{}").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 0);
    }

    #[test]
    fn test_single_field() {
        let tokens = lex("{id: 1}").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 1);

        let field = &row[0];
        assert_eq!(field.key.value(), "id");
        let Literal(Number(value)) = field.value.as_ref() else { panic!() };
        assert_eq!(value.value(), "1");
    }

    #[test]
    fn test_multiple_fields() {
        let tokens = lex(r#"{id: 1, name: 'Ada'}"#).unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 2);

        let id_field = &row[0];
        assert_eq!(id_field.key.value(), "id");
        let Literal(Number(value)) = id_field.value.as_ref() else { panic!() };
        assert_eq!(value.value(), "1");

        let name_field = &row[1];
        assert_eq!(name_field.key.value(), "name");
        let Literal(Text(value)) = name_field.value.as_ref() else { panic!() };
        assert_eq!(value.value(), "Ada");
    }

    #[test]
    fn test_identifier_value() {
        let tokens = lex("{field: someVariable}").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 1);

        let field = &row[0];
        assert_eq!(field.key.value(), "field");
        let Identifier(identifier) = field.value.as_ref() else { panic!() };
        assert_eq!(identifier.value(), "someVariable");
    }

    #[test]
    fn test_multiline_row() {
        let tokens = lex(r#"{
            id: 42,
            name: 'Database',
            active: true
        }"#)
        .unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 3);

        let id_field = &row[0];
        assert_eq!(id_field.key.value(), "id");
        let Literal(Number(value)) = id_field.value.as_ref() else { panic!() };
        assert_eq!(value.value(), "42");

        let name_field = &row[1];
        assert_eq!(name_field.key.value(), "name");
        let Literal(Text(value)) = name_field.value.as_ref() else { panic!() };
        assert_eq!(value.value(), "Database");

        let active_field = &row[2];
        assert_eq!(active_field.key.value(), "active");
        assert!(active_field.value.is_literal_boolean());
    }

    #[test]
    fn test_trailing_comma() {
        let tokens = lex("{id: 1, name: 'Test',}").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 2);

        let id_field = &row[0];
        assert_eq!(id_field.key.value(), "id");

        let name_field = &row[1];
        assert_eq!(name_field.key.value(), "name");
    }

    #[test]
    fn test_complex_values() {
        let tokens = lex("{result: (1 + 2), enabled: !false}").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 2);

        let result_field = &row[0];
        assert_eq!(result_field.key.value(), "result");
        assert!(result_field.value.is_tuple());

        let enabled_field = &row[1];
        assert_eq!(enabled_field.key.value(), "enabled");
        assert!(enabled_field.value.is_prefix());
    }

    #[test]
    fn test_nested_row() {
        let tokens = lex("{user: {id: 1, name: 'John'}}").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let row = result[0].first_unchecked().as_block();
        assert_eq!(row.len(), 1);

        let user_field = &row[0];
        assert_eq!(user_field.key.value(), "user");
        assert!(user_field.value.is_block());

        let nested_row = user_field.value.as_block();
        assert_eq!(nested_row.len(), 2);
        assert_eq!(nested_row[0].key.value(), "id");
        assert_eq!(nested_row[1].key.value(), "name");
    }
}
