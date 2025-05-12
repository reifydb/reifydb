// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Operator::{CloseParen, OpenParen};
use crate::ast::lex::Token;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstBlock, parse};

impl Parser {
    pub(crate) fn parse_block(&mut self) -> parse::Result<AstBlock> {
        let token = self.consume_operator(OpenParen)?;
        let result = self.parse_block_inner(token)?;
        self.consume_operator(CloseParen)?;
        Ok(result)
    }

    pub(crate) fn parse_block_inner(&mut self, token: Token) -> parse::Result<AstBlock> {
        let mut nodes = Vec::new();
        loop {
            self.skip_new_line()?;
            if self.current()?.is_operator(CloseParen) {
                break;
            }
            nodes.push(self.parse_node(Precedence::None)?);
        }
        Ok(AstBlock { token, nodes })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Ast::Literal;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::ast::{AstFrom, AstLiteral};

    #[test]
    fn empty_block() {
        let tokens = lex("FROM ()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstFrom::Query { query, .. } = result[0].as_from() else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes, vec![]);
    }

    #[test]
    fn block_with_white_spaces() {
        let tokens = lex("FROM (    \t     )").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstFrom::Query { query, .. } = result[0].as_from() else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes, vec![]);
    }

    #[test]
    fn block_with_new_lines() {
        let tokens = lex(r#"FROM (


        )"#)
        .unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstFrom::Query { query, .. } = result[0].as_from() else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes, vec![]);
    }

    #[test]
    fn block_nested() {
        let tokens = lex(r#" FROM (
        FROM (      )
        )"#)
        .unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let AstFrom::Query { query, .. } = result[0].as_from() else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes.len(), 1);

        let from = block.nodes[0].as_from();
        let AstFrom::Query { query, .. } = from else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes.len(), 0);
    }

    #[test]
    #[ignore]
    fn block_with_comments() {
        let tokens = lex(r#"FROM (
        // before
        FROM ()
        // after
        )"#)
        .unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let block = result[0].as_block();
        assert_eq!(block.nodes.len(), 1);

        let block = block.nodes[0].as_block();
        assert_eq!(block.nodes.len(), 0);
    }

    #[test]
    fn block_multilayer_nested() {
        let tokens = lex(r#"FROM ( FROM(   FROM(  true )   ))"#).unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let from = result[0].as_from();
        let AstFrom::Query { query, .. } = from else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes.len(), 1);

        let from = block.nodes[0].as_from();
        let AstFrom::Query { query, .. } = from else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes.len(), 1);

        let from = block.nodes[0].as_from();
        let AstFrom::Query { query, .. } = from else { panic!() };
        let block = query.as_block();
        assert_eq!(block.nodes.len(), 1);

        let Literal(AstLiteral::Boolean(boolean_node)) = block.nodes.first().unwrap() else {
            panic!()
        };
        assert!(boolean_node.value());
    }
}
