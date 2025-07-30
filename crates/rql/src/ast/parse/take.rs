// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Keyword;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{Ast, AstLiteral, AstTake};

impl Parser {
    pub(crate) fn parse_take(&mut self) -> crate::Result<AstTake> {
        let token = self.consume_keyword(Keyword::Take)?;
        let take = self.parse_node(Precedence::None)?;
        match take {
            Ast::Literal(literal) => match literal {
                AstLiteral::Number(number) => {
                    Ok(AstTake { token, take: number.value().parse().unwrap() })
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
    fn test_take_number() {
        let tokens = lex("TAKE 10").unwrap();
        let mut parser = Parser::new(tokens);
        let mut result = parser.parse().unwrap();
        assert_eq!(result.len(), 1);

        let result = result.pop().unwrap();
        let take = result.first_unchecked().as_take();
        assert_eq!(take.take, 10);
    }
}
