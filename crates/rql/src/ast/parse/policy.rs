// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::ast::lex::Keyword::Policy;
use crate::ast::lex::{Literal, Operator, Separator};
use crate::ast::parse::{Parser, Precedence, invalid_policy_error};
use crate::ast::{AstPolicy, AstPolicyBlock, AstPolicyKind, Token, TokenKind, parse};
use Separator::Comma;
use TokenKind::Identifier;


impl Parser {
    pub(crate) fn parse_policy_block(&mut self) -> parse::Result<AstPolicyBlock> {
        let token = self.consume_keyword(Policy)?;
        self.consume_operator(Operator::OpenParen)?;

        let mut policies = Vec::new();
        loop {
            let (token, policy) = self.parse_policy_kind()?;
            let value = Box::new(self.parse_node(Precedence::None)?);

            policies.push(AstPolicy { token, policy, value });

            if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
                break;
            }
        }

        self.consume_operator(Operator::CloseParen)?;
        Ok(AstPolicyBlock { token, policies })
    }

    fn parse_policy_kind(&mut self) -> parse::Result<(Token, AstPolicyKind)> {
        let identifier = self.consume(Identifier)?;
        let ty = match identifier.span.fragment.as_str() {
            "saturation" => AstPolicyKind::Saturation,
            "default" => AstPolicyKind::Default,
            "not" => {
                self.consume_literal(Literal::Undefined)?;
                AstPolicyKind::NotUndefined
            }
            _ => return Err(invalid_policy_error(identifier)),
        };

        Ok((identifier, ty))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;
    use crate::ast::{AstCreate, AstCreateTable, AstPolicyKind};

    #[test]
    fn test_saturation_error() {
        let tokens = lex(r#"policy (saturation error)"#).unwrap();

        let mut parser = Parser::new(tokens);
        let result = parser.parse_policy_block().unwrap();
        assert_eq!(result.policies.len(), 1);

        let policies = result.policies;
        assert_eq!(policies.len(), 1);

        let saturation = &policies[0];
        assert!(matches!(saturation.policy, AstPolicyKind::Saturation));
        assert_eq!(saturation.value.as_identifier().value(), "error");
    }

    #[test]
    fn test_saturation_undefined() {
        let tokens = lex(r#"policy (saturation undefined)"#).unwrap();

        let mut parser = Parser::new(tokens);
        let result = parser.parse_policy_block().unwrap();
        assert_eq!(result.policies.len(), 1);

        let policies = result.policies;
        assert_eq!(policies.len(), 1);

        let saturation = &policies[0];
        assert!(matches!(saturation.policy, AstPolicyKind::Saturation));
        assert_eq!(saturation.value.as_literal_undefined().value(), "undefined");
    }

    #[test]
    fn test_table_with_policy_block() {
        let tokens = lex(r#"
        create table test.items(
            field:  int2
                    policy (
                        saturation error,
                        default 0
                    )
        )
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

                let policies = col.policies.as_ref().unwrap();
                assert_eq!(policies.policies.len(), 2);

                let saturation = &policies.policies[0];
                assert!(matches!(saturation.policy, AstPolicyKind::Saturation));
                assert_eq!(saturation.value.as_identifier().value(), "error");

                let default = &policies.policies[1];
                assert!(matches!(default.policy, AstPolicyKind::Default));
                assert_eq!(default.value.value(), "0");
            }
            _ => unreachable!(),
        }
    }
}
