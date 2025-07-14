// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Literal::{False, Number, Text, True, Undefined};
use crate::ast::lex::Separator::NewLine;
use crate::ast::lex::{Keyword, Operator, TokenKind};
use crate::ast::parse::{Error, Parser};
use crate::ast::{Ast, AstWildcard, parse};

impl Parser {
    pub(crate) fn parse_primary(&mut self) -> parse::Result<Ast> {
        loop {
            if self.is_eof() {
                return Ok(Ast::Nop);
            }

            let is_new_line = self.current()?.is_separator(NewLine);
            if !is_new_line {
                break;
            }
            let _ = self.advance()?;
        }

        let current = self.current()?;
        match &current.kind {
            TokenKind::Operator(operator) => match operator {
                Operator::Plus | Operator::Minus | Operator::Bang => self.parse_prefix(),
                Operator::Asterisk => Ok(Ast::Wildcard(AstWildcard(self.advance()?))),
                Operator::OpenBracket => Ok(Ast::List(self.parse_list()?)),
                Operator::OpenParen => Ok(Ast::Tuple(self.parse_tuple()?)),
                Operator::OpenCurly => Ok(Ast::Row(self.parse_row()?)),
                _ => Err(Error::unsupported(self.advance()?)),
            },
            TokenKind::Keyword(keyword) => match keyword {
                Keyword::From => Ok(Ast::From(self.parse_from()?)),
                Keyword::Map => Ok(Ast::Map(self.parse_map()?)),
                Keyword::Filter => Ok(Ast::Filter(self.parse_filter()?)),
                Keyword::Aggregate => Ok(Ast::Aggregate(self.parse_group_by()?)),
                Keyword::Cast => Ok(Ast::Cast(self.parse_cast()?)),
                Keyword::Create => Ok(Ast::Create(self.parse_create()?)),
                Keyword::Insert => Ok(Ast::InsertIntoTable(self.parse_insert()?)),
                Keyword::Left => Ok(Ast::Join(self.parse_left_join()?)),
                Keyword::Take => Ok(Ast::Take(self.parse_take()?)),
                Keyword::Sort => Ok(Ast::Sort(self.parse_sort()?)),
                Keyword::Policy => Ok(Ast::PolicyBlock(self.parse_policy_block()?)),
                Keyword::Describe => Ok(Ast::Describe(self.parse_describe()?)),
                _ => Err(Error::unsupported(self.advance()?)),
            },
            _ => match current {
                _ if current.is_literal(Number) => Ok(Ast::Literal(self.parse_literal_number()?)),
                _ if current.is_literal(True) => Ok(Ast::Literal(self.parse_literal_true()?)),
                _ if current.is_literal(False) => Ok(Ast::Literal(self.parse_literal_false()?)),
                _ if current.is_literal(Text) => Ok(Ast::Literal(self.parse_literal_text()?)),
                _ if current.is_literal(Undefined) => {
                    Ok(Ast::Literal(self.parse_literal_undefined()?))
                }
                _ if current.is_identifier() => match self.parse_kind() {
                    Ok(node) => Ok(Ast::DataType(node)),
                    Err(_) => Ok(Ast::Identifier(self.parse_identifier()?)),
                },
                _ => Err(Error::unsupported(self.advance()?)),
            },
        }
    }
}
