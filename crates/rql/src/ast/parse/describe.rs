// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword::Describe;
use crate::ast::lex::Operator;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstDescribe, parse};

impl Parser {
    pub(crate) fn parse_describe(&mut self) -> parse::Result<AstDescribe> {
        let token = self.consume_keyword(Describe)?;
        self.consume_operator(Operator::OpenParen)?;
        let node = Box::new(self.parse_node(Precedence::None)?);
        self.consume_operator(Operator::CloseParen)?;
        Ok(AstDescribe::Query { token, node })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::ast::{AstCast, AstDescribe};

    #[test]
    fn describe_query() {
        let tokens = lex("describe ( select cast(9924 as int8) )").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        match result.first().unwrap().first_unchecked().as_describe() {
            AstDescribe::Query { node, .. } => {
                let select = node.as_select();
                assert_eq!(select.columns.len(), 1);

                let AstCast { node, to, .. } = select.columns[0].as_cast();

                let number = node.as_literal_number();
                assert_eq!(number.value(), "9924");
                assert_eq!(to.value(), "int8");
            }
        };
    }
}
