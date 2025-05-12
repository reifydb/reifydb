// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Literal::{False, Number, Text, True, Undefined};
use crate::ast::lex::Separator::NewLine;
use crate::ast::lex::{Keyword, Operator, TokenKind};
use crate::ast::parse::{Error, Parser, Precedence};
use crate::ast::{Ast, AstPrefix, AstWildcard, PrefixOperator, parse};

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
                Operator::Plus | Operator::Minus | Operator::Bang => {
                    let operator = self.parse_prefix_operator()?;
                    Ok(Ast::Prefix(AstPrefix {
                        operator,
                        node: Box::new(self.parse_node(Precedence::None)?),
                    }))
                }
                Operator::Asterisk => Ok(Ast::Wildcard(AstWildcard(self.advance()?))),
                Operator::OpenParen => Ok(Ast::Tuple(self.parse_tuple()?)),
                _ => Err(Error::unsupported(self.advance()?)),
            },
            TokenKind::Keyword(keyword) => match keyword {
                Keyword::Create => Ok(Ast::Create(self.parse_create()?)),
                Keyword::From => Ok(Ast::From(self.parse_from()?)),
                Keyword::Insert => Ok(Ast::Insert(self.parse_insert()?)),
                Keyword::Limit => Ok(Ast::Limit(self.parse_limit()?)),
                Keyword::Select => Ok(Ast::Select(self.parse_select()?)),
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
                _ if current.is_identifier() => match self.parse_type() {
                    Ok(node) => Ok(Ast::Type(node)),
                    Err(_) => Ok(Ast::Identifier(self.parse_identifier()?)),
                },
                _ => Err(Error::unsupported(self.advance()?)),
            },
        }
    }

    pub(crate) fn parse_prefix_operator(&mut self) -> parse::Result<PrefixOperator> {
        let token = self.advance()?;
        match &token.kind {
            TokenKind::Operator(operator) => match operator {
                Operator::Plus => Ok(PrefixOperator::Plus(token)),
                Operator::Minus => Ok(PrefixOperator::Negate(token)),
                Operator::Bang => Ok(PrefixOperator::Not(token)),
                _ => Err(Error::unsupported(token)),
            },
            _ => Err(Error::unsupported(token)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Ast::Literal;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::ast::{Ast, AstLiteral, AstPrefix, PrefixOperator};
    use std::ops::Deref;

    #[test]
    fn test_plus() {
        let tokens = lex("+2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { ref operator, ref node }) = result[0] else { panic!() };
        assert!(matches!(*operator, PrefixOperator::Plus(_)));

        let Literal(AstLiteral::Number(node)) = node.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_negate() {
        let tokens = lex("-2").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { ref operator, ref node }) = result[0] else { panic!() };
        assert!(matches!(*operator, PrefixOperator::Negate(_)));

        let Literal(AstLiteral::Number(node)) = node.deref() else { panic!() };
        assert_eq!(node.value(), "2");
    }

    #[test]
    fn test_not() {
        let tokens = lex("!false").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Ast::Prefix(AstPrefix { ref operator, ref node }) = result[0] else { panic!() };
        assert!(matches!(*operator, PrefixOperator::Not(_)));

        let Literal(AstLiteral::Boolean(node)) = node.deref() else { panic!() };
        assert_eq!(node.value(), false);
    }
}
