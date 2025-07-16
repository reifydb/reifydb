// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword::{Join, Left, On, With};
use crate::ast::lex::Operator::{CloseCurly, OpenCurly};
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Error, Parser, Precedence};
use crate::ast::{AstJoin, parse};
use reifydb_core::diagnostic::parse::multiple_expressions_without_braces;

impl Parser {
    pub(crate) fn parse_left_join(&mut self) -> parse::Result<AstJoin> {
        let token = self.consume_keyword(Left)?;
        self.consume_keyword(Join)?;

        let has_braces = self.current()?.is_operator(OpenCurly);

        if has_braces {
            self.advance()?;
        }

        self.consume_keyword(With)?;
        let with = Box::new(self.parse_node(Precedence::None)?);

        self.consume_keyword(On)?;

        let has_on_braces = self.current()?.is_operator(OpenCurly);

        if has_on_braces {
            self.advance()?;
        }

        let mut on = Vec::new();
        loop {
            on.push(self.parse_node(Precedence::None)?);

            if self.is_eof() {
                break;
            }

            if has_on_braces && self.current()?.is_operator(CloseCurly) {
                self.advance()?;
                break;
            }

            if self.current()?.is_separator(Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        if on.len() > 1 && !has_on_braces {
            return Err(Error::Passthrough {
                diagnostic: multiple_expressions_without_braces(token.span),
            });
        }

        if has_braces {
            if !self.is_eof() && self.current()?.is_operator(CloseCurly) {
                self.advance()?;
            }
        }

        Ok(AstJoin::LeftJoin { token, with, on })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::Parser;
    use crate::ast::{AstJoin, InfixOperator};

    #[test]
    fn test_left_join_old_syntax() {
        let tokens = lex("left join with schema.orders on user.id == orders.user_id").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let join = result.first_unchecked().as_join();

        let AstJoin::LeftJoin { with, on, .. } = &join;
        let with = with.as_infix();
        assert_eq!(with.left.as_identifier().value(), "schema");
        assert!(matches!(with.operator, InfixOperator::AccessTable(_)));
        assert_eq!(with.right.as_identifier().value(), "orders");

        assert_eq!(on.len(), 1);
        let on = on[0].as_infix();
        {
            let left = on.left.as_infix();
            assert_eq!(left.left.as_identifier().value(), "user");
            assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
            assert_eq!(left.right.as_identifier().value(), "id");
        }

        assert!(matches!(on.operator, InfixOperator::Equal(_)));

        {
            let right = on.right.as_infix();
            assert_eq!(right.left.as_identifier().value(), "orders");
            assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
            assert_eq!(right.right.as_identifier().value(), "user_id");
        }
    }

    #[test]
    fn test_left_join_new_syntax() {
        let tokens = lex("left join { with orders on { users.id == orders.user_id, something_else.id == orders.user_id } }").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let join = result.first_unchecked().as_join();

        let AstJoin::LeftJoin { with, on, .. } = &join;
        assert_eq!(with.as_identifier().value(), "orders");

        assert_eq!(on.len(), 2);

        // First condition: users.id == orders.user_id
        let on1 = on[0].as_infix();
        {
            let left = on1.left.as_infix();
            assert_eq!(left.left.as_identifier().value(), "users");
            assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
            assert_eq!(left.right.as_identifier().value(), "id");
        }
        assert!(matches!(on1.operator, InfixOperator::Equal(_)));
        {
            let right = on1.right.as_infix();
            assert_eq!(right.left.as_identifier().value(), "orders");
            assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
            assert_eq!(right.right.as_identifier().value(), "user_id");
        }

        // Second condition: something_else.id == orders.user_id
        let on2 = on[1].as_infix();
        {
            let left = on2.left.as_infix();
            assert_eq!(left.left.as_identifier().value(), "something_else");
            assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
            assert_eq!(left.right.as_identifier().value(), "id");
        }
        assert!(matches!(on2.operator, InfixOperator::Equal(_)));
        {
            let right = on2.right.as_infix();
            assert_eq!(right.left.as_identifier().value(), "orders");
            assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
            assert_eq!(right.right.as_identifier().value(), "user_id");
        }
    }

    #[test]
    fn test_left_join_single_on_with_braces() {
        let tokens = lex("left join { with orders on { users.id == orders.user_id } }").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let join = result.first_unchecked().as_join();

        let AstJoin::LeftJoin { with, on, .. } = &join;
        assert_eq!(with.as_identifier().value(), "orders");
        assert_eq!(on.len(), 1);
    }

    #[test]
    fn test_left_join_multiple_on_without_braces_fails() {
        let tokens = lex("left join with orders on users.id == orders.user_id, something_else.id == orders.user_id").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse();

        assert!(result.is_err(), "Expected error for multiple ON conditions without braces");
    }
}
