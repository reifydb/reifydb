// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Literal::{False, Number, Temporal, Text, True, Undefined};
use crate::ast::lex::Separator::NewLine;
use crate::ast::lex::{Keyword, Operator, TokenKind};
use crate::ast::parse::Parser;
use crate::ast::parse::error::unsupported_token_error;
use crate::ast::{Ast, AstWildcard};
use reifydb_core::return_error;

impl Parser {
    pub(crate) fn parse_primary(&mut self) -> crate::Result<Ast> {
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
                Operator::Plus | Operator::Minus | Operator::Bang | Operator::Not => self.parse_prefix(),
                Operator::Asterisk => Ok(Ast::Wildcard(AstWildcard(self.advance()?))),
                Operator::OpenBracket => Ok(Ast::List(self.parse_list()?)),
                Operator::OpenParen => Ok(Ast::Tuple(self.parse_tuple()?)),
                Operator::OpenCurly => Ok(Ast::Inline(self.parse_inline()?)),
                _ => return_error!(unsupported_token_error(self.advance()?)),
            },
            TokenKind::Keyword(keyword) => match keyword {
                Keyword::From => Ok(Ast::From(self.parse_from()?)),
                Keyword::Map => Ok(Ast::Map(self.parse_map()?)),
                Keyword::Filter => Ok(Ast::Filter(self.parse_filter()?)),
                Keyword::Aggregate => Ok(Ast::Aggregate(self.parse_aggregate()?)),
                Keyword::Cast => Ok(Ast::Cast(self.parse_cast()?)),
                Keyword::Create => Ok(Ast::Create(self.parse_create()?)),
                Keyword::Delete => Ok(Ast::AstDelete(self.parse_delete()?)),
                Keyword::Insert => Ok(Ast::AstInsert(self.parse_insert()?)),
                Keyword::Update => Ok(Ast::AstUpdate(self.parse_update()?)),
                Keyword::Inner => Ok(Ast::Join(self.parse_inner_join()?)),
                Keyword::Join => Ok(Ast::Join(self.parse_join()?)),
                Keyword::Left => Ok(Ast::Join(self.parse_left_join()?)),
                Keyword::Natural => Ok(Ast::Join(self.parse_natural_join()?)),
                Keyword::Take => Ok(Ast::Take(self.parse_take()?)),
                Keyword::Sort => Ok(Ast::Sort(self.parse_sort()?)),
                Keyword::Policy => Ok(Ast::PolicyBlock(self.parse_policy_block()?)),
                Keyword::Describe => Ok(Ast::Describe(self.parse_describe()?)),
                _ => return_error!(unsupported_token_error(self.advance()?)),
            },
            _ => match current {
                _ if current.is_literal(Number) => Ok(Ast::Literal(self.parse_literal_number()?)),
                _ if current.is_literal(True) => Ok(Ast::Literal(self.parse_literal_true()?)),
                _ if current.is_literal(False) => Ok(Ast::Literal(self.parse_literal_false()?)),
                _ if current.is_literal(Text) => Ok(Ast::Literal(self.parse_literal_text()?)),
                _ if current.is_literal(Temporal) => {
                    Ok(Ast::Literal(self.parse_literal_temporal()?))
                }
                _ if current.is_literal(Undefined) => {
                    Ok(Ast::Literal(self.parse_literal_undefined()?))
                }
                _ if current.is_identifier() => {
                    if self.is_function_call_pattern() {
                        Ok(Ast::CallFunction(self.parse_function_call()?))
                    } else {
                        Ok(Ast::Identifier(self.parse_identifier()?))
                    }
                },
                _ => return_error!(unsupported_token_error(self.advance()?)),
            },
        }
    }
}
