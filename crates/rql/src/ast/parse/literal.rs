// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Literal;
use crate::ast::parse::Parser;
use crate::ast::{
    AstLiteral, AstLiteralBoolean, AstLiteralNumber, AstLiteralText, AstLiteralUndefined, parse,
};

impl Parser {
    pub(crate) fn parse_literal_number(&mut self) -> parse::Result<AstLiteral> {
        let token = self.consume_literal(Literal::Number)?;
        Ok(AstLiteral::Number(AstLiteralNumber(token)))
    }

    pub(crate) fn parse_literal_text(&mut self) -> parse::Result<AstLiteral> {
        let token = self.consume_literal(Literal::Text)?;
        Ok(AstLiteral::Text(AstLiteralText(token)))
    }

    pub(crate) fn parse_literal_true(&mut self) -> parse::Result<AstLiteral> {
        let token = self.consume_literal(Literal::True)?;
        Ok(AstLiteral::Boolean(AstLiteralBoolean(token)))
    }

    pub(crate) fn parse_literal_false(&mut self) -> parse::Result<AstLiteral> {
        let token = self.consume_literal(Literal::False)?;
        Ok(AstLiteral::Boolean(AstLiteralBoolean(token)))
    }

    pub(crate) fn parse_literal_undefined(&mut self) -> parse::Result<AstLiteral> {
        let token = self.consume_literal(Literal::Undefined)?;
        Ok(AstLiteral::Undefined(AstLiteralUndefined(token)))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Ast::Literal;
    use crate::ast::AstLiteral;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;

    #[test]
    fn test_text() {
        let tokens = lex("'ElodiE'").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Text(node)) = &result[0] else { panic!() };
        assert_eq!(node.value(), "ElodiE");
    }

    #[test]
    fn test_number_42() {
        let tokens = lex("42").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Number(node)) = &result[0] else { panic!() };
        assert_eq!(node.value(), "42");
    }

    #[test]
    fn test_true() {
        let tokens = lex("true").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Boolean(node)) = &result[0] else { panic!() };
        assert!(node.value());
    }

    #[test]
    fn test_false() {
        let tokens = lex("false").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Boolean(node)) = &result[0] else { panic!() };
        assert!(!node.value());
    }
}
