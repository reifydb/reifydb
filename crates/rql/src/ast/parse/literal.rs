// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Literal;
use crate::ast::parse::Parser;
use crate::ast::{
    AstLiteral, AstLiteralBoolean, AstLiteralNumber, AstLiteralTemporal, AstLiteralText, AstLiteralUndefined, parse,
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

    pub(crate) fn parse_literal_temporal(&mut self) -> parse::Result<AstLiteral> {
        let token = self.consume_literal(Literal::Temporal)?;
        Ok(AstLiteral::Temporal(AstLiteralTemporal(token)))
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

        let Literal(AstLiteral::Text(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "ElodiE");
    }

    #[test]
    fn test_number_42() {
        let tokens = lex("42").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Number(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "42");
    }

    #[test]
    fn test_true() {
        let tokens = lex("true").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Boolean(node)) = &result[0].first_unchecked() else { panic!() };
        assert!(node.value());
    }

    #[test]
    fn test_false() {
        let tokens = lex("false").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Boolean(node)) = &result[0].first_unchecked() else { panic!() };
        assert!(!node.value());
    }

    #[test]
    fn test_date() {
        let tokens = lex("@2024-03-15").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "2024-03-15");
    }

    #[test]
    fn test_time() {
        let tokens = lex("@14:30:00").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "14:30:00");
    }

    #[test]
    fn test_datetime() {
        let tokens = lex("@2024-03-15T14:30:00").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "2024-03-15T14:30:00");
    }

    #[test]
    fn test_range_interval_date() {
        let tokens = lex("@2024-03-15..2024-03-16").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "2024-03-15..2024-03-16");
    }

    #[test]
    fn test_range_interval_time() {
        let tokens = lex("@14:30:00..15:30:00").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "14:30:00..15:30:00");
    }

    #[test]
    fn test_range_interval_datetime() {
        let tokens = lex("@2024-03-15T14:30:00..2024-03-15T15:30:00").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "2024-03-15T14:30:00..2024-03-15T15:30:00");
    }

    #[test]
    fn test_mixed_range_interval() {
        let tokens = lex("@2024-03-15..14:30:00").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "2024-03-15..14:30:00");
    }

    #[test]
    fn test_duration_interval_date() {
        let tokens = lex("@P1D").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "P1D");
    }

    #[test]
    fn test_duration_interval_time() {
        let tokens = lex("@PT2H30M").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "PT2H30M");
    }

    #[test]
    fn test_duration_interval_datetime() {
        let tokens = lex("@P1Y2M3DT4H5M6S").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else { panic!() };
        assert_eq!(node.value(), "P1Y2M3DT4H5M6S");
    }
}
