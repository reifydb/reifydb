// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword::{Join, Left, On};
use crate::ast::lex::Separator::Comma;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstJoin, parse};

impl Parser {
    pub(crate) fn parse_left_join(&mut self) -> parse::Result<AstJoin> {
        let token = self.consume_keyword(Left)?;
        self.consume_keyword(Join)?;
        let with = Box::new(self.parse_node(Precedence::None)?);
        self.consume_keyword(On)?;

        let mut on = Vec::new();
        loop {
            on.push(self.parse_node(Precedence::None)?);

            if self.is_eof() {
                break;
            }

            // consume comma and continue
            if self.current()?.is_separator(Comma) {
                self.advance()?;
            } else {
                break;
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
    fn test_left_join() {
        let tokens = lex("left join schema.orders on user.id == orders.user_id").unwrap();
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
}
