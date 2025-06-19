// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Operator::OpenParen;
use crate::ast::parse::Error::InvalidType;
use crate::ast::parse::Parser;
use crate::ast::{AstKind, parse};

impl Parser {
    pub(crate) fn parse_kind(&mut self) -> parse::Result<AstKind> {
        let value = self.current()?.span.fragment.as_str();

        let constructor = if value.eq_ignore_ascii_case("BOOL") {
            AstKind::Boolean
        } else if value.eq_ignore_ascii_case("FLOAT4") {
            AstKind::Float4
        } else if value.eq_ignore_ascii_case("FLOAT8") {
            AstKind::Float8
        } else if value.eq_ignore_ascii_case("INT1") {
            AstKind::Int1
        } else if value.eq_ignore_ascii_case("INT2") {
            AstKind::Int2
        } else if value.eq_ignore_ascii_case("INT4") {
            AstKind::Int4
        } else if value.eq_ignore_ascii_case("INT8") {
            AstKind::Int8
        } else if value.eq_ignore_ascii_case("INT16") {
            AstKind::Int16
        } else if value.eq_ignore_ascii_case("NUMBER") {
            AstKind::Number
        } else if value.eq_ignore_ascii_case("TEXT") {
            AstKind::Text
        } else if value.eq_ignore_ascii_case("UINT1") {
            AstKind::Uint1
        } else if value.eq_ignore_ascii_case("UINT2") {
            AstKind::Uint2
        } else if value.eq_ignore_ascii_case("UINT4") {
            AstKind::Uint4
        } else if value.eq_ignore_ascii_case("UINT8") {
            AstKind::Uint8
        } else if value.eq_ignore_ascii_case("UINT16") {
            AstKind::Uint16
        } else {
            let token = self.current()?;
            return Err(InvalidType { got: token.clone() });
        };

        // consume only after confirming match
        let token = self.advance()?;

        if !self.is_eof() && self.current()?.is_operator(OpenParen) {
            // For now simply ignore additional type information like TEXT(255)
            self.parse_tuple()?;
        }

        Ok(constructor(token))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstKind;
    use crate::ast::lex::lex;
    use crate::ast::parse::Error::InvalidType;
    use crate::ast::parse::Parser;

    #[test]
    fn test_not_a_type() {
        let tokens = lex("something_different").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind();
        let Err(InvalidType { .. }) = result else { panic!() };
    }

    #[test]
    fn test_type_boolean() {
        let tokens = lex("Bool").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Boolean(_) = result else { panic!() };
    }

    #[test]
    fn test_type_float4() {
        let tokens = lex("Float4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Float4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_float8() {
        let tokens = lex("Float8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Float8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int1() {
        let tokens = lex("Int1").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Int1(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int2() {
        let tokens = lex("Int2").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Int2(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int4() {
        let tokens = lex("Int4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Int4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int8() {
        let tokens = lex("Int8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Int8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint1() {
        let tokens = lex("Uint1").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Uint1(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint2() {
        let tokens = lex("Uint2").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Uint2(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint4() {
        let tokens = lex("Uint4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Uint4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint8() {
        let tokens = lex("Uint8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Uint8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_number() {
        let tokens = lex("Number").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Number(_) = result else { panic!() };
    }

    #[test]
    fn test_type_text() {
        let tokens = lex("Text").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Text(_) = result else { panic!() };
    }

    #[test]
    fn test_type_text_bound() {
        let tokens = lex("Text(255)").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_kind().unwrap();
        let AstKind::Text(_) = result else { panic!() };
        assert!(parser.is_eof())
    }
}
