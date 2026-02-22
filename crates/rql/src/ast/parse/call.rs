// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstCall, AstCallFunction},
		identifier::MaybeQualifiedFunctionIdentifier,
		parse::Parser,
	},
	token::{keyword::Keyword, operator::Operator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_call(&mut self) -> crate::Result<AstCall<'bump>> {
		let token = self.consume_keyword(Keyword::Call)?;

		let first_ident = self.consume(crate::token::token::TokenKind::Identifier)?;
		let mut namespace_fragments = Vec::new();

		// Parse optional namespace chain: ident::ident::...::ident
		let mut current = first_ident;
		while self.current()?.is_operator(Operator::DoubleColon) {
			namespace_fragments.push(current.fragment);
			self.advance()?; // consume ::
			current = if self.current()?.is_identifier() {
				self.consume(crate::token::token::TokenKind::Identifier)?
			} else {
				self.consume_keyword_as_ident()?
			};
		}

		let function = if namespace_fragments.is_empty() {
			MaybeQualifiedFunctionIdentifier::new(current.fragment)
		} else {
			MaybeQualifiedFunctionIdentifier::new(current.fragment).with_namespaces(namespace_fragments)
		};

		let arguments = self.parse_tuple()?;

		Ok(AstCall {
			token,
			function,
			arguments,
		})
	}

	pub(crate) fn parse_function_call(&mut self) -> crate::Result<AstCallFunction<'bump>> {
		let first_ident_token = self.consume(crate::token::token::TokenKind::Identifier)?;
		let start_token = first_ident_token;

		// Check if this is a simple function call: identifier(
		if self.current()?.is_operator(Operator::OpenParen) {
			// Simple function call like func()
			let open_paren_token = self.advance()?; // Consume the opening parenthesis

			let arguments = self.parse_tuple_call(open_paren_token)?;

			// Create MaybeQualifiedFunctionIdentifier without
			// namespaces
			let function = MaybeQualifiedFunctionIdentifier::new(first_ident_token.fragment);

			return Ok(AstCallFunction {
				token: start_token,
				function,
				arguments,
			});
		}

		// Collect namespace chain:
		// identifier::identifier::...::identifier( The
		// first_ident_token we consumed is part of the namespace
		// chain
		let mut current_ident_token = first_ident_token;
		let mut namespace_fragments = Vec::new();

		while self.current()?.is_operator(Operator::DoubleColon) {
			// Add current identifier to namespace chain before
			// parsing next
			namespace_fragments.push(current_ident_token.fragment);

			self.advance()?; // consume ::
			let next_ident_token = if self.current()?.is_identifier() {
				self.consume(crate::token::token::TokenKind::Identifier)?
			} else {
				self.consume_keyword_as_ident()?
			};

			// Check if this is the function name (followed by
			// opening paren)
			if self.current()?.is_operator(Operator::OpenParen) {
				// This is the function name, parse arguments
				let open_paren_token = self.advance()?; // Consume the opening parenthesis

				let arguments = self.parse_tuple_call(open_paren_token)?;

				// Create MaybeQualifiedFunctionIdentifier with
				// namespaces
				let function = MaybeQualifiedFunctionIdentifier::new(next_ident_token.fragment)
					.with_namespaces(namespace_fragments);

				return Ok(AstCallFunction {
					token: start_token,
					function,
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
		unreachable!("parse_function_call called on non-function call pattern")
	}

	pub(crate) fn is_function_call_pattern(&self) -> bool {
		let tokens_len = self.tokens.len();
		if self.position >= tokens_len {
			return false;
		}

		if !unsafe { self.tokens.get_unchecked(self.position) }.is_identifier() {
			return false;
		}

		if self.position + 1 < tokens_len
			&& unsafe { self.tokens.get_unchecked(self.position + 1) }.is_operator(Operator::OpenParen)
		{
			return true;
		}

		let mut pos = self.position + 1;
		while pos + 2 < tokens_len {
			if !unsafe { self.tokens.get_unchecked(pos) }.is_operator(Operator::DoubleColon) {
				return false;
			}
			pos += 1;

			let token = unsafe { self.tokens.get_unchecked(pos) };
			if !token.is_identifier() && !token.is_keyword_as_ident() {
				return false;
			}
			pos += 1;

			if unsafe { self.tokens.get_unchecked(pos) }.is_operator(Operator::OpenParen) {
				return true;
			}
		}

		false
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{ast::Ast, parse::parse},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_namespaced_call() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CALL ns::greet()").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);
		let Ast::Call(call) = result[0].first_unchecked() else {
			panic!("expected Call")
		};
		assert_eq!(call.function.name.text(), "greet");
		assert_eq!(call.function.namespaces.len(), 1);
		assert_eq!(call.function.namespaces[0].text(), "ns");
	}

	#[test]
	fn test_simple_call() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CALL greet()").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);
		let Ast::Call(call) = result[0].first_unchecked() else {
			panic!("expected Call")
		};
		assert_eq!(call.function.name.text(), "greet");
		assert!(call.function.namespaces.is_empty());
	}

	#[test]
	fn test_simple_function_call() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "func()").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let call = result[0].first_unchecked().as_call_function();
		assert_eq!(call.function.name.text(), "func");
		assert!(call.function.namespaces.is_empty());
		assert_eq!(call.arguments.len(), 0);
	}

	#[test]
	fn test_function_call_with_args() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "func('arg')").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let call = result[0].first_unchecked().as_call_function();
		assert_eq!(call.function.name.text(), "func");
		assert!(call.function.namespaces.is_empty());
		assert_eq!(call.arguments.len(), 1);
	}

	#[test]
	fn test_namespaced_function_call() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "blob::hex('deadbeef')").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let call = result[0].first_unchecked().as_call_function();
		assert_eq!(call.function.name.text(), "hex");
		assert_eq!(call.function.namespaces.len(), 1);
		assert_eq!(call.function.namespaces[0].text(), "blob");
		assert_eq!(call.arguments.len(), 1);
	}

	#[test]
	fn test_deeply_nested_function_call() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ext::crypto::hash::sha256('data')").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let call = result[0].first_unchecked().as_call_function();
		assert_eq!(call.function.name.text(), "sha256");
		assert_eq!(call.function.namespaces.len(), 3);
		assert_eq!(call.function.namespaces[0].text(), "ext");
		assert_eq!(call.function.namespaces[1].text(), "crypto");
		assert_eq!(call.function.namespaces[2].text(), "hash");
		assert_eq!(call.arguments.len(), 1);
	}

	#[test]
	fn test_identifier_without_parens_not_function_call() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "identifier").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		// Should be parsed as identifier, not function call
		assert!(result[0].first_unchecked().as_identifier().text() == "identifier");
	}

	#[test]
	fn test_namespaced_function_call_with_keyword_name() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "clock::set(1000)").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let call = result[0].first_unchecked().as_call_function();
		assert_eq!(call.function.name.text(), "set");
		assert_eq!(call.function.namespaces.len(), 1);
		assert_eq!(call.function.namespaces[0].text(), "clock");
	}

	#[test]
	fn test_namespace_access_without_parens_not_function_call() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "namespace::identifier").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		// Should be parsed as infix expression, not function call
		let infix = result[0].first_unchecked().as_infix();
		assert_eq!(infix.left.as_identifier().text(), "namespace");
		assert_eq!(infix.right.as_identifier().text(), "identifier");
	}
}
