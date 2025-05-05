// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::parse;
use crate::rql::parse::node::NodeType;
use crate::rql::parse::Error::InvalidType;
use crate::rql::parse::Parser;

impl Parser {
    pub(crate) fn parse_type(&mut self) -> parse::Result<NodeType> {
        let value = self.current()?.span.fragment.as_str();

        let constructor = if value.eq_ignore_ascii_case("Bool") {
            NodeType::Boolean
        } else if value.eq_ignore_ascii_case("Float4") {
            NodeType::Float4
        } else if value.eq_ignore_ascii_case("Float8") {
            NodeType::Float8
        } else if value.eq_ignore_ascii_case("Int1") {
            NodeType::Int1
        } else if value.eq_ignore_ascii_case("Int2") {
            NodeType::Int2
        } else if value.eq_ignore_ascii_case("Int4") {
            NodeType::Int4
        } else if value.eq_ignore_ascii_case("Int8") {
            NodeType::Int8
        } else if value.eq_ignore_ascii_case("Int16") {
            NodeType::Int16
        } else if value.eq_ignore_ascii_case("Number") {
            NodeType::Number
        } else if value.eq_ignore_ascii_case("Text") {
            NodeType::Text
        } else if value.eq_ignore_ascii_case("Uint1") {
            NodeType::Uint1
        } else if value.eq_ignore_ascii_case("Uint2") {
            NodeType::Uint2
        } else if value.eq_ignore_ascii_case("Uint4") {
            NodeType::Uint4
        } else if value.eq_ignore_ascii_case("Uint8") {
            NodeType::Uint8
        } else if value.eq_ignore_ascii_case("Uint16") {
            NodeType::Uint16
        } else {
            let token = self.current().unwrap(); // already checked it exists
            return Err(InvalidType(token.clone()));
        };

        // consume only after confirming match
        let token = self.advance()?;
        Ok(constructor(token))
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::lex::lex;
    use crate::rql::parse::node::NodeType;
    use crate::rql::parse::Error::InvalidType;
    use crate::rql::parse::Parser;

    #[test]
    fn test_not_a_type() {
        let tokens = lex("something_different").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type();
        let Err(InvalidType(_)) = result else { panic!() };
    }

    #[test]
    fn test_type_boolean() {
        let tokens = lex("Bool").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Boolean(_) = result else { panic!() };
    }

    #[test]
    fn test_type_float4() {
        let tokens = lex("Float4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Float4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_float8() {
        let tokens = lex("Float8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Float8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int1() {
        let tokens = lex("Int1").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Int1(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int2() {
        let tokens = lex("Int2").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Int2(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int4() {
        let tokens = lex("Int4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Int4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_int8() {
        let tokens = lex("Int8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Int8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint1() {
        let tokens = lex("Uint1").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Uint1(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint2() {
        let tokens = lex("Uint2").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Uint2(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint4() {
        let tokens = lex("Uint4").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Uint4(_) = result else { panic!() };
    }

    #[test]
    fn test_type_uint8() {
        let tokens = lex("Uint8").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Uint8(_) = result else { panic!() };
    }

    #[test]
    fn test_type_number() {
        let tokens = lex("Number").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Number(_) = result else { panic!() };
    }

    #[test]
    fn test_type_text() {
        let tokens = lex("Text").unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse_type().unwrap();
        let NodeType::Text(_) = result else { panic!() };
    }
}
