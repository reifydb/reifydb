// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstCallFunction;
use crate::ast::lex::Operator;
use crate::ast::parse::Parser;

impl Parser {
    pub(crate) fn parse_function_call(&mut self) -> crate::Result<AstCallFunction> {
        let mut namespaces = Vec::new();
        let first_ident = self.parse_identifier()?;
        let start_token = first_ident.0.clone();

        // Check if this is a simple function call: identifier(
        if self.current()?.is_operator(Operator::OpenParen) {
            // Simple function call like func()
            let open_paren_token = self.advance()?; // Consume the opening parenthesis
            let arguments = self.parse_tuple_call(open_paren_token)?;
            return Ok(AstCallFunction {
                token: start_token,
                namespaces: Vec::new(),
                function: first_ident,
                arguments,
            });
        }

        // Collect namespace chain: identifier::identifier::...::identifier(
        // The first_ident we parsed is part of the namespace chain
        let mut current_ident = first_ident;

        while self.current()?.is_operator(Operator::DoubleColon) {
            // Add current identifier to namespace chain before parsing next
            namespaces.push(current_ident);

            self.advance()?; // consume ::
            let next_ident = self.parse_identifier()?;

            // Check if this is the function name (followed by opening paren)
            if self.current()?.is_operator(Operator::OpenParen) {
                // This is the function name, parse arguments
                let open_paren_token = self.advance()?; // Consume the opening parenthesis
                let arguments = self.parse_tuple_call(open_paren_token)?;
                return Ok(AstCallFunction {
                    token: start_token,
                    namespaces,
                    function: next_ident,
                    arguments,
                });
            } else {
                // Continue with next identifier in the chain
                current_ident = next_ident;
            }
        }

        // If we get here, we have namespace::identifier but no opening paren
        // This means it's not a function call, so we should not have called this method
        // This shouldn't happen if lookahead logic is correct
        unreachable!("parse_function_call called on non-function call pattern")
    }

    pub(crate) fn is_function_call_pattern(&self) -> bool {
        // Now I understand: tokens are in REVERSE order!
        // For "func()", tokens are: [), (, func] - indices [0, 1, 2]
        // current() returns tokens[2] which is the identifier

        let tokens_len = self.tokens.len();
        if tokens_len < 2 {
            return false; // Need at least identifier + open paren
        }

        // Check if current token (last in vector) is identifier
        if !self.tokens[tokens_len - 1].is_identifier() {
            return false;
        }

        // Check if the token before current (moving backwards) is open paren
        if self.tokens[tokens_len - 2].is_operator(Operator::OpenParen) {
            return true; // Simple function call: func()
        }

        // For namespaced calls: "blob::hex()" tokens are [), (, hex, ::, blob]
        // We need to match pattern: identifier :: identifier ( )
        // The tokens in reverse are: [), (, identifier, ::, identifier, ::, ...]

        if tokens_len < 4 {
            return false; // Need at least: namespace :: func (
        }

        // Current token is identifier (func name)
        // Next token should be ::
        if !self.tokens[tokens_len - 2].is_operator(Operator::DoubleColon) {
            return false;
        }

        // Look for the pattern backwards: identifier :: identifier :: ... ( )
        let mut i = tokens_len - 3; // Start checking from the namespace identifier

        loop {
            // Should be an identifier (namespace part)
            if i >= tokens_len || !self.tokens[i].is_identifier() {
                return false;
            }

            if i == 0 {
                return false; // Need something before identifier
            }
            i -= 1;

            // Check what follows this identifier
            if self.tokens[i].is_operator(Operator::OpenParen) {
                return true; // Found pattern: namespace::func(
            }

            // Should be another :: for deeper nesting
            if !self.tokens[i].is_operator(Operator::DoubleColon) {
                return false;
            }

            if i == 0 {
                return false;
            }
            i -= 1; // Move to next identifier in the chain
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::lex;
    use crate::ast::parse::parse;

    #[test]
    fn test_simple_function_call() {
        let tokens = lex("func()").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let call = result[0].first_unchecked().as_call_function();
        assert_eq!(call.function.value(), "func");
        assert!(call.namespaces.is_empty());
        assert_eq!(call.arguments.len(), 0);
    }

    #[test]
    fn test_function_call_with_args() {
        let tokens = lex("func('arg')").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let call = result[0].first_unchecked().as_call_function();
        assert_eq!(call.function.value(), "func");
        assert!(call.namespaces.is_empty());
        assert_eq!(call.arguments.len(), 1);
    }

    #[test]
    fn test_namespaced_function_call() {
        let tokens = lex("blob::hex('deadbeef')").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let call = result[0].first_unchecked().as_call_function();
        assert_eq!(call.function.value(), "hex");
        assert_eq!(call.namespaces.len(), 1);
        assert_eq!(call.namespaces[0].value(), "blob");
        assert_eq!(call.arguments.len(), 1);
    }

    #[test]
    fn test_deeply_nested_function_call() {
        let tokens = lex("ext::crypto::hash::sha256('data')").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        let call = result[0].first_unchecked().as_call_function();
        assert_eq!(call.function.value(), "sha256");
        assert_eq!(call.namespaces.len(), 3);
        assert_eq!(call.namespaces[0].value(), "ext");
        assert_eq!(call.namespaces[1].value(), "crypto");
        assert_eq!(call.namespaces[2].value(), "hash");
        assert_eq!(call.arguments.len(), 1);
    }

    #[test]
    fn test_identifier_without_parens_not_function_call() {
        let tokens = lex("identifier").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        // Should be parsed as identifier, not function call
        assert!(result[0].first_unchecked().as_identifier().value() == "identifier");
    }

    #[test]
    fn test_namespace_access_without_parens_not_function_call() {
        let tokens = lex("namespace::identifier").unwrap();
        let result = parse(tokens).unwrap();
        assert_eq!(result.len(), 1);

        // Should be parsed as infix expression, not function call
        let infix = result[0].first_unchecked().as_infix();
        assert_eq!(infix.left.as_identifier().value(), "namespace");
        assert_eq!(infix.right.as_identifier().value(), "identifier");
    }
}
