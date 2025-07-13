// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::ast::AstIdentifier;
use crate::ast::lex::TokenKind;
use crate::ast::parse;
use crate::ast::parse::Parser;

impl Parser {
    pub(crate) fn parse_identifier(&mut self) -> parse::Result<AstIdentifier> {
        let token = self.consume(TokenKind::Identifier)?;
        Ok(AstIdentifier(token))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::ast::AstIdentifier;
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;
    use crate::ast::Ast::Identifier;

    #[test]
    fn identifier() {
        let tokens = lex("x").unwrap();
        let mut result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Identifier(AstIdentifier(token)) = result.pop().unwrap().0.pop().unwrap() else { panic!() };
        assert_eq!(token.value(), "x");
    }

    #[test]
    fn identifier_with_underscore() {
        let tokens = lex("some_identifier").unwrap();
        let mut result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let Identifier(AstIdentifier(token)) = result.pop().unwrap().0.pop().unwrap() else { panic!() };
        assert_eq!(token.value(), "some_identifier");
    }
}
