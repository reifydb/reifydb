// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::parse::{Parser, Precedence};

#[derive(Debug, PartialEq)]
pub(crate) enum Node {}

impl<'a> Parser<'a> {
    pub(crate) fn parse_node(&mut self, precedence: Precedence) -> crate::rql::frontend::parse::Result<Node> {
        todo!()
        // let mut left = self.parse_primary()?;
        //
        // while !self.is_eof() && precedence < self.current_precedence()? {
        //     left = Node::Infix(self.parse_infix(left)?);
        // }
        // Ok(left)
    }
}
