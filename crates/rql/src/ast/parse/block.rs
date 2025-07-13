// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::ast::lex::Operator;
use crate::ast::lex::Operator::CloseParen;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{AstBlock, parse};

impl Parser {
    pub(crate) fn parse_block(&mut self) -> parse::Result<AstBlock> {
        let token = self.consume_operator(Operator::OpenParen)?;

        let mut nodes = Vec::new();
        loop {
            self.skip_new_line()?;

            if self.current()?.is_operator(CloseParen) {
                break;
            }
            nodes.push(self.parse_node(Precedence::None)?);
        }

        self.consume_operator(CloseParen)?;
        Ok(AstBlock { token, nodes })
    }
}
