// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{Ast, AstLimit, AstLiteral, parse};

impl Parser {
    pub(crate) fn parse_limit(&mut self) -> parse::Result<AstLimit> {
        let token = self.consume_keyword(Keyword::Limit)?;
        let limit = self.parse_node(Precedence::None)?;
        match limit {
            Ast::Literal(literal) => match literal {
                AstLiteral::Number(number) => {
                    return Ok(AstLimit { token, limit: number.value().parse().unwrap() });
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::lex;

    #[test]
    fn test_limit_number() {
        let tokens = lex("LIMIT 10").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let limit = result.as_limit();
        assert_eq!(limit.limit, 10);
    }
}
