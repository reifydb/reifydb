// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstCall, AstCallFunction,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_call(&mut self) -> crate::Result<AstCall<'a>> {
		let token = self.consume_keyword(Keyword::Call)?;

		// Parse the operator name (e.g., counter, sequence,
		// running_sum)
		let operator_name = self.parse_identifier()?;

		// Parse arguments if present
		let arguments = self.parse_tuple()?;

		Ok(AstCall {
			token,
			operator_name,
			arguments,
		})
	}

	pub(crate) fn parse_function_call(
		&mut self,
	) -> crate::Result<AstCallFunction<'a>> {
		let mut namespaces = Vec::with_capacity(2);
		let first_ident_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;
		let start_token = first_ident_token.clone();

		// Check if this is a simple function call: identifier(
		if self.current()?.is_operator(Operator::OpenParen) {
			// Simple function call like func()
			let open_paren_token = self.advance()?; // Consume the opening parenthesis

			let arguments =
				self.parse_tuple_call(open_paren_token)?;

			let first_ident = crate::ast::ast::AstIdentifier(
				first_ident_token,
			);
			return Ok(AstCallFunction {
				token: start_token,
				namespaces: Vec::new(),
				function: first_ident,
				arguments,
			});
		}

		// Collect namespace chain:
		// identifier::identifier::...::identifier( The
		// first_ident_token we consumed is part of the namespace
		// chain
		let mut current_ident_token = first_ident_token;

		while self.current()?.is_operator(Operator::DoubleColon) {
			// Add current identifier to namespace chain before
			// parsing next
			namespaces.push(crate::ast::ast::AstIdentifier(
				current_ident_token,
			));

			self.advance()?; // consume ::
			let next_ident_token = self.consume(
				crate::ast::tokenize::TokenKind::Identifier,
			)?;

			// Check if this is the function name (followed by
			// opening paren)
			if self.current()?.is_operator(Operator::OpenParen) {
				// This is the function name, parse arguments
				let open_paren_token = self.advance()?; // Consume the opening parenthesis

				let arguments = self
					.parse_tuple_call(open_paren_token)?;

				let next_ident = crate::ast::ast::AstIdentifier(
					next_ident_token,
				);
				return Ok(AstCallFunction {
					token: start_token,
					namespaces,
					function: next_ident,
					arguments,
				});
			} else {
				// Continue with next identifier in the chain
				current_ident_token = next_ident_token;
			}
		}

		// If we get here, we have namespace::identifier but no opening
		// paren This means it's not a function call, so we should not
		// have called this method This shouldn't happen if lookahead
		// logic is correct
		unreachable!(
			"parse_function_call called on non-function call pattern"
		)
	}

	pub(crate) fn is_function_call_pattern(&self) -> bool {
		let tokens_len = self.tokens.len();
		if self.position >= tokens_len {
			return false;
		}

		if !unsafe { self.tokens.get_unchecked(self.position) }
			.is_identifier()
		{
			return false;
		}

		if self.position + 1 < tokens_len
			&& unsafe {
				self.tokens.get_unchecked(self.position + 1)
			}
			.is_operator(Operator::OpenParen)
		{
			return true;
		}

		let mut pos = self.position + 1;
		while pos + 2 < tokens_len {
			if !unsafe { self.tokens.get_unchecked(pos) }
				.is_operator(Operator::DoubleColon)
			{
				return false;
			}
			pos += 1;

			if !unsafe { self.tokens.get_unchecked(pos) }
				.is_identifier()
			{
				return false;
			}
			pos += 1;

			if unsafe { self.tokens.get_unchecked(pos) }
				.is_operator(Operator::OpenParen)
			{
				return true;
			}
		}

		false
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{parse::parse, tokenize::tokenize};

	#[test]
	fn test_simple_function_call() {
		let tokens = tokenize("func()").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let call = result[0].first_unchecked().as_call_function();
		assert_eq!(call.function.value(), "func");
		assert!(call.namespaces.is_empty());
		assert_eq!(call.arguments.len(), 0);
	}

	#[test]
	fn test_function_call_with_args() {
		let tokens = tokenize("func('arg')").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let call = result[0].first_unchecked().as_call_function();
		assert_eq!(call.function.value(), "func");
		assert!(call.namespaces.is_empty());
		assert_eq!(call.arguments.len(), 1);
	}

	#[test]
	fn test_namespaced_function_call() {
		let tokens = tokenize("blob::hex('deadbeef')").unwrap();
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
		let tokens =
			tokenize("ext::crypto::hash::sha256('data')").unwrap();
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
		let tokens = tokenize("identifier").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		// Should be parsed as identifier, not function call
		assert!(result[0].first_unchecked().as_identifier().value()
			== "identifier");
	}

	#[test]
	fn test_namespace_access_without_parens_not_function_call() {
		let tokens = tokenize("namespace::identifier").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		// Should be parsed as infix expression, not function call
		let infix = result[0].first_unchecked().as_infix();
		assert_eq!(infix.left.as_identifier().value(), "namespace");
		assert_eq!(infix.right.as_identifier().value(), "identifier");
	}
}
