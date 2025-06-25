// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword::Cast;
use crate::ast::lex::Operator;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstCast, parse};

impl Parser {
    pub(crate) fn parse_cast(&mut self) -> parse::Result<AstCast> {
        let token = self.consume_keyword(Cast)?;
        self.consume_operator(Operator::OpenParen)?;
        let node = Box::new(self.parse_node(Precedence::None)?);
        self.consume_operator(Operator::As)?;
        let to = self.parse_kind()?;
        self.consume_operator(Operator::CloseParen)?;

        Ok(AstCast { token, node, to })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstCast;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;

    #[test]
    fn test_cast() {
        let tokens = lex("cast(9924 as int8)").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstCast { node, to, .. } = result[0].first_unchecked().as_cast();

        let number = node.as_literal_number();
        assert_eq!(number.value(), "9924");
        assert_eq!(to.value(), "int8");
    }
}
