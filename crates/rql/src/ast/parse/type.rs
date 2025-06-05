// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::parse::Error::InvalidType;
use crate::ast::parse::Parser;
use crate::ast::{AstType, parse};

impl Parser {
    pub(crate) fn parse_type(&mut self) -> parse::Result<AstType> {
        let value = self.current()?.span.fragment.as_str();

        let constructor = if value.eq_ignore_ascii_case("BOOL") {
            AstType::Boolean
        } else if value.eq_ignore_ascii_case("FLOAT4") {
            AstType::Float4
        } else if value.eq_ignore_ascii_case("FLOAT8") {
            AstType::Float8
        } else if value.eq_ignore_ascii_case("INT1") {
            AstType::Int1
        } else if value.eq_ignore_ascii_case("INT2") {
            AstType::Int2
        } else if value.eq_ignore_ascii_case("INT4") {
            AstType::Int4
        } else if value.eq_ignore_ascii_case("INT8") {
            AstType::Int8
        } else if value.eq_ignore_ascii_case("INT16") {
            AstType::Int16
        } else if value.eq_ignore_ascii_case("NUMBER") {
            AstType::Number
        } else if value.eq_ignore_ascii_case("TEXT") {
            AstType::Text
        } else if value.eq_ignore_ascii_case("UINT1") {
            AstType::Uint1
        } else if value.eq_ignore_ascii_case("UINT2") {
            AstType::Uint2
        } else if value.eq_ignore_ascii_case("UINT4") {
            AstType::Uint4
        } else if value.eq_ignore_ascii_case("UINT8") {
            AstType::Uint8
        } else if value.eq_ignore_ascii_case("UINT16") {
            AstType::Uint16
        } else {
            let token = self.current()?;
            return Err(InvalidType { got: token.clone() });
        };

        // consume only after confirming match
        let token = self.advance()?;
        Ok(constructor(token))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstType;
    use crate::ast::lex::lex;
    use crate::ast::parse::Error::InvalidType;
    use crate::ast::parse::Parser;

    #[test]
    fn test_not_a_type() {
        let tokens = lex("something_different").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type();
        let Err(InvalidType { .. }) = result else { panic!() };
    }

    #[test]
    fn test_type_boolean() {
        let tokens = lex("Bool").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Boolean(_) = result else { panic!() };
    }

    #[test]
    fn test_type_float4() {
        let tokens = lex("Float4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Float4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_float8() {
        let tokens = lex("Float8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Float8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int1() {
        let tokens = lex("Int1").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Int1(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int2() {
        let tokens = lex("Int2").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Int2(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int4() {
        let tokens = lex("Int4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Int4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int8() {
        let tokens = lex("Int8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Int8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint1() {
        let tokens = lex("Uint1").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Uint1(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint2() {
        let tokens = lex("Uint2").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Uint2(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint4() {
        let tokens = lex("Uint4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Uint4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint8() {
        let tokens = lex("Uint8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Uint8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_number() {
        let tokens = lex("Number").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Number(_) = result else { panic!() };
    }

    #[test]
    fn test_type_text() {
        let tokens = lex("Text").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let AstType::Text(_) = result else { panic!() };
    }
}
