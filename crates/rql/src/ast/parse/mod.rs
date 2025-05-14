// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod create;
mod error;
mod from;
mod identifier;
mod infix;
mod insert;
mod limit;
mod literal;
mod primary;
mod select;
mod tuple;
mod r#type;

pub use error::*;

use crate::ast::Ast;
use crate::ast::lex::Separator::NewLine;
use crate::ast::lex::{Keyword, Literal, Operator, Separator, Token, TokenKind};
use crate::ast::parse::Error::UnexpectedEndOfFile;
use std::cmp::PartialOrd;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(crate) enum Precedence {
    None,
    Assignment,
    LogicalOr,
    LogicalAnd,
    Equality,
    Comparison,
    Term,
    Factor,
    Unary,
    Call,
    Primary,
}

pub type Result<T> = core::result::Result<T, Error>;

pub(crate) fn parse<'a>(tokens: Vec<Token>) -> Result<Vec<Ast>> {
    let mut parser = Parser::new(tokens);
    parser.parse()
}

struct Parser {
    tokens: Vec<Token>,
    precedence_map: HashMap<Operator, Precedence>,
}

impl Parser {
    fn new(mut tokens: Vec<Token>) -> Self {
        let mut precedence_map = HashMap::new();
        precedence_map.insert(Operator::Equal, Precedence::Assignment);

        precedence_map.insert(Operator::DoubleEqual, Precedence::Comparison);
        precedence_map.insert(Operator::BangEqual, Precedence::Comparison);

        precedence_map.insert(Operator::LeftAngle, Precedence::Comparison);
        precedence_map.insert(Operator::LeftAngleEqual, Precedence::Comparison);
        precedence_map.insert(Operator::RightAngle, Precedence::Comparison);
        precedence_map.insert(Operator::RightAngleEqual, Precedence::Comparison);

        precedence_map.insert(Operator::Plus, Precedence::Term);
        precedence_map.insert(Operator::Minus, Precedence::Term);

        precedence_map.insert(Operator::Asterisk, Precedence::Factor);
        precedence_map.insert(Operator::Slash, Precedence::Factor);
        precedence_map.insert(Operator::Percent, Precedence::Factor);

        precedence_map.insert(Operator::OpenParen, Precedence::Call);

        precedence_map.insert(Operator::Dot, Precedence::Primary);
        precedence_map.insert(Operator::DoubleColon, Precedence::Primary);

        precedence_map.insert(Operator::Arrow, Precedence::Primary);
        precedence_map.insert(Operator::Colon, Precedence::Primary);

        tokens.reverse();
        Self { tokens, precedence_map }
    }

    fn parse(&mut self) -> Result<Vec<Ast>> {
        let mut nodes = vec![];
        loop {
            if self.is_eof() {
                break;
            }
            nodes.push(self.parse_node(Precedence::None)?);
            if !self.is_eof() {
                self.consume_if(TokenKind::Separator(NewLine))?;
            }
        }
        Ok(nodes)
    }

    pub(crate) fn parse_node(&mut self, precedence: Precedence) -> Result<Ast> {
        let mut left = self.parse_primary()?;

        while !self.is_eof() && precedence < self.current_precedence()? {
            left = Ast::Infix(self.parse_infix(left)?);
        }
        Ok(left)
    }

    pub(crate) fn advance(&mut self) -> Result<Token> {
        self.tokens.pop().ok_or(Error::eof())
    }

    pub(crate) fn consume(&mut self, expected: TokenKind) -> Result<Token> {
        self.current_expect(expected)?;
        self.advance()
    }

    pub(crate) fn consume_if(&mut self, expected: TokenKind) -> Result<Option<Token>> {
        if self.is_eof() || self.current()?.kind != expected {
            return Ok(None);
        }

        Ok(Some(self.consume(expected)?))
    }

    pub(crate) fn consume_while(&mut self, expected: TokenKind) -> Result<()> {
        loop {
            if self.is_eof() || self.current()?.kind != expected {
                return Ok(());
            }
            self.advance()?;
        }
    }

    pub(crate) fn consume_literal(&mut self, expected: Literal) -> Result<Token> {
        self.current_expect_literal(expected)?;
        self.advance()
    }

    pub(crate) fn consume_operator(&mut self, expected: Operator) -> Result<Token> {
        self.current_expect_operator(expected)?;
        self.advance()
    }

    pub(crate) fn consume_keyword(&mut self, expected: Keyword) -> Result<Token> {
        self.current_expect_keyword(expected)?;
        self.advance()
    }

    pub(crate) fn consume_separator(&mut self, expected: Separator) -> Result<Token> {
        self.current_expect_separator(expected)?;
        self.advance()
    }

    pub(crate) fn current(&self) -> Result<&Token> {
        self.tokens.last().ok_or(UnexpectedEndOfFile)
    }

    pub(crate) fn current_expect(&self, expected: TokenKind) -> Result<()> {
        let got = self.current()?;
        if got.kind == expected { Ok(()) } else { Err(Error::unexpected(expected, got.clone())) }
    }

    pub(crate) fn current_expect_literal(&self, literal: Literal) -> Result<()> {
        self.current_expect(TokenKind::Literal(literal))
    }

    pub(crate) fn current_expect_operator(&self, operator: Operator) -> Result<()> {
        self.current_expect(TokenKind::Operator(operator))
    }

    pub(crate) fn current_expect_keyword(&self, keyword: Keyword) -> Result<()> {
        self.current_expect(TokenKind::Keyword(keyword))
    }

    pub(crate) fn current_expect_separator(&self, separator: Separator) -> Result<()> {
        self.current_expect(TokenKind::Separator(separator))
    }

    pub(crate) fn current_precedence(&self) -> Result<Precedence> {
        if self.is_eof() {
            return Ok(Precedence::None);
        };

        let current = self.current()?;
        if let TokenKind::Operator(operator) = current.kind {
            let precedence = self.precedence_map.get(&operator).cloned();
            Ok(precedence.unwrap_or(Precedence::None))
        } else {
            Ok(Precedence::None)
        }
    }

    pub(crate) fn peek_next(&self) -> Result<&Token> {
        if self.tokens.len() < 2 {
            return Err(Error::eof());
        }
        self.tokens.get(self.tokens.len() - 2).ok_or(Error::eof())
    }

    pub(crate) fn peek_next_expect(&self, expected: TokenKind) -> Result<()> {
        let got = self.peek_next()?;
        if got.kind == expected { Ok(()) } else { Err(Error::unexpected(expected, got.clone())) }
    }

    fn is_eof(&self) -> bool {
        self.tokens.is_empty()
    }

    pub(crate) fn skip_new_line(&mut self) -> Result<()> {
        self.consume_while(TokenKind::Separator(NewLine))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::Literal::{False, Number, True};
    use crate::ast::lex::Operator::Plus;
    use crate::ast::lex::Separator::Semicolon;
    use crate::ast::lex::TokenKind::{Identifier, Literal, Separator};
    use crate::ast::lex::{TokenKind, lex};
    use crate::ast::parse::Error::UnexpectedEndOfFile;
    use crate::ast::parse::Precedence::Term;
    use crate::ast::parse::{Error, Parser, Precedence};

    #[test]
    fn test_advance_but_eof() {
        let mut parser = Parser::new(vec![]);
        let result = parser.advance();
        assert_eq!(result, Err(UnexpectedEndOfFile))
    }

    #[test]
    fn test_advance() {
        let tokens = lex("1 + 2").unwrap();
        let mut parser = Parser::new(tokens);

        let one = parser.advance().unwrap();
        assert_eq!(one.kind, Literal(Number));
        assert_eq!(one.span.fragment, "1");

        let plus = parser.advance().unwrap();
        assert_eq!(plus.kind, TokenKind::Operator(Plus));
        assert_eq!(plus.span.fragment, "+");

        let two = parser.advance().unwrap();
        assert_eq!(two.kind, Literal(Number));
        assert_eq!(two.span.fragment, "2");
    }

    #[test]
    fn test_consume_but_eof() {
        let tokens = lex("").unwrap();
        let mut parser = Parser::new(tokens);
        let err = parser.consume(Identifier).err().unwrap();
        assert_eq!(err, UnexpectedEndOfFile)
    }

    #[test]
    fn test_consume_but_unexpected_token() {
        let tokens = lex("false").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.consume(Literal(True));
        assert!(result.is_err());

        if let Error::UnexpectedToken { expected, got, .. } = result.err().unwrap() {
            assert_eq!(expected, Literal(True));
            assert_eq!(got.kind, Literal(False));
        }
    }

    #[test]
    fn test_consume() {
        let tokens = lex("true 99").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.consume(Literal(True)).unwrap();
        assert_eq!(result.kind, Literal(True));

        let result = parser.consume(Literal(Number)).unwrap();
        assert_eq!(result.kind, Literal(Number));
    }

    #[test]
    fn test_consume_if_but_eof() {
        let tokens = lex("").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.consume_if(Literal(True));
        assert_eq!(result, Ok(None))
    }

    #[test]
    fn test_consume_if_but_unexpected_token() {
        let tokens = lex("false").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.consume_if(Literal(True));
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_consume_if() {
        let tokens = lex("true 0x99").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.consume_if(Literal(True)).unwrap().unwrap();
        assert_eq!(result.kind, Literal(True));

        let result = parser.consume_if(Literal(Number)).unwrap().unwrap();
        assert_eq!(result.kind, Literal(Number));
    }

    #[test]
    fn test_current_but_eof() {
        let tokens = lex("").unwrap();
        let parser = Parser::new(tokens);
        let result = parser.current();
        assert_eq!(result, Err(UnexpectedEndOfFile))
    }

    #[test]
    fn test_current() {
        let tokens = lex("true false").unwrap();
        let mut parser = Parser::new(tokens);

        let true_token = parser.current().unwrap().clone();
        assert_eq!(true_token.kind, Literal(True));

        parser.advance().unwrap();

        let false_token = parser.current().unwrap().clone();
        assert_eq!(false_token.kind, Literal(False));
    }

    #[test]
    fn test_current_expect_but_eof() {
        let tokens = lex("").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.current_expect(Separator(Semicolon));
        assert_eq!(result, Err(UnexpectedEndOfFile))
    }

    #[test]
    fn test_current_expect() {
        let tokens = lex("true false").unwrap();
        let mut parser = Parser::new(tokens);

        let result = parser.current_expect(Literal(True));
        assert!(result.is_ok());

        parser.advance().unwrap();

        let result = parser.current_expect(Literal(False));
        assert!(result.is_ok());
    }

    #[test]
    fn test_current_expect_but_different() {
        let tokens = lex("true").unwrap();
        let parser = Parser::new(tokens);

        let result = parser.current_expect(Literal(False));
        assert!(result.is_err());

        if let Error::UnexpectedToken { expected, got, .. } = result.err().unwrap() {
            assert_eq!(expected, Literal(False));
            assert_eq!(got.kind, Literal(True));
        }
    }

    #[test]
    fn test_current_precedence_but_eof() {
        let tokens = lex("").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.current_precedence();
        assert_eq!(result, Ok(Precedence::None))
    }

    #[test]
    fn test_current_precedence() {
        let tokens = lex("+").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.current_precedence();
        assert_eq!(result, Ok(Term))
    }

    #[test]
    fn test_peek_next_but_eof() {
        let tokens = lex("").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.peek_next();
        assert_eq!(result, Err(UnexpectedEndOfFile))
    }

    #[test]
    fn test_peek_next_but_nothing_to_peek_next() {
        let tokens = lex("true").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.peek_next();
        assert_eq!(result, Err(UnexpectedEndOfFile))
    }

    #[test]
    fn test_peek_next() {
        let tokens = lex("true false 1").unwrap();
        let mut parser = Parser::new(tokens);

        let false_token = parser.peek_next().unwrap().clone();
        assert_eq!(false_token.kind, Literal(False));

        parser.advance().unwrap();
        let number_token = parser.peek_next().unwrap().clone();
        assert_eq!(number_token.kind, Literal(Number));
    }

    #[test]
    fn test_peek_next_expect_but_eof() {
        let tokens = lex("").unwrap();
        let parser = Parser::new(tokens);
        let result = parser.peek_next_expect(Separator(Semicolon));
        assert_eq!(result, Err(UnexpectedEndOfFile))
    }

    #[test]
    fn test_peek_next_expect_but_nothing_to_peek_next() {
        let tokens = lex("true").unwrap();
        let parser = Parser::new(tokens);

        let result = parser.peek_next_expect(Separator(Semicolon));
        assert_eq!(result, Err(UnexpectedEndOfFile));
    }

    #[test]
    fn test_peek_next_expect() {
        let tokens = lex("true false 0o123").unwrap();
        let mut parser = Parser::new(tokens);

        let result = parser.peek_next_expect(Literal(False));
        assert!(result.is_ok());

        parser.advance().unwrap();

        let result = parser.peek_next_expect(Literal(Number));
        assert!(result.is_ok());
    }

    #[test]
    fn test_peek_next_expect_but_different() {
        let tokens = lex("true 0b111").unwrap();
        let mut parser = Parser::new(tokens);

        let result = parser.peek_next_expect(Literal(False));
        assert!(result.is_err());

        if let Error::UnexpectedToken { expected, got, .. } = result.err().unwrap() {
            assert_eq!(expected, Literal(False));
            assert_eq!(got.kind, Literal(Number))
        }
    }
}
