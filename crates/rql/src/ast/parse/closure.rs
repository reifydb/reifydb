// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstClosure, AstFunctionParameter, AstVariable},
		parse::Parser,
	},
	token::{operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse a closure: `($params) { body }`
	/// Called when we see '(' and determine it's a closure (not a tuple or grouped expression).
	pub(crate) fn parse_closure(&mut self) -> crate::Result<AstClosure<'bump>> {
		let token = self.consume_operator(Operator::OpenParen)?;

		// Parse parameters (same format as function params)
		let parameters = self.parse_closure_parameters()?;

		self.consume_operator(Operator::CloseParen)?;

		// Parse body block
		let body = self.parse_block()?;

		Ok(AstClosure {
			token,
			parameters,
			body,
		})
	}

	/// Parse closure parameters: $var, $var2: type
	fn parse_closure_parameters(&mut self) -> crate::Result<Vec<AstFunctionParameter<'bump>>> {
		let mut parameters = Vec::new();

		loop {
			self.skip_new_line()?;

			// Check for closing paren (empty params or trailing comma)
			if self.current()?.is_operator(Operator::CloseParen) {
				break;
			}

			// Parse parameter: $name or $name: type
			let param_token = self.consume(TokenKind::Variable)?;
			let variable = AstVariable {
				token: param_token,
			};

			// Optional type annotation: : type
			let type_annotation = if !self.is_eof() && self.current()?.is_operator(Operator::Colon) {
				self.consume_operator(Operator::Colon)?;
				Some(self.parse_type_annotation()?)
			} else {
				None
			};

			parameters.push(AstFunctionParameter {
				token: param_token,
				variable,
				type_annotation,
			});

			self.skip_new_line()?;

			// Check for comma to continue, or break if no comma
			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}

			// Check for closing paren
			if self.current()?.is_operator(Operator::CloseParen) {
				break;
			}
		}

		Ok(parameters)
	}

	/// Determine if the current position starts a closure.
	/// A closure starts with '(' followed by either ')' '{' (empty params)
	/// or '$var' patterns that look like parameters rather than expressions.
	pub(crate) fn is_closure_pattern(&self) -> bool {
		if self.position >= self.tokens.len() {
			return false;
		}

		// Must start with '('
		if !self.tokens[self.position].is_operator(Operator::OpenParen) {
			return false;
		}

		let mut pos = self.position + 1;

		// Check for empty closure: () {
		if pos < self.tokens.len() && self.tokens[pos].is_operator(Operator::CloseParen) {
			pos += 1;
			return pos < self.tokens.len() && self.tokens[pos].is_operator(Operator::OpenCurly);
		}

		// Check for ($var pattern
		if pos < self.tokens.len() && matches!(self.tokens[pos].kind, TokenKind::Variable) {
			// Scan forward to find closing paren, checking if this looks like params
			pos += 1;

			// Skip optional type annotation
			if pos < self.tokens.len() && self.tokens[pos].is_operator(Operator::Colon) {
				pos += 1; // skip colon
				// Skip type name
				if pos < self.tokens.len() {
					pos += 1;
				}
			}

			// Check for ')' followed by '{'
			loop {
				if pos >= self.tokens.len() {
					return false;
				}

				if self.tokens[pos].is_operator(Operator::CloseParen) {
					pos += 1;
					return pos < self.tokens.len()
						&& self.tokens[pos].is_operator(Operator::OpenCurly);
				}

				// Allow comma-separated params
				if self.tokens[pos].is_separator(Separator::Comma) {
					pos += 1;
					// Next should be a variable
					if pos < self.tokens.len()
						&& matches!(self.tokens[pos].kind, TokenKind::Variable)
					{
						pos += 1;
						// Skip optional type annotation
						if pos < self.tokens.len()
							&& self.tokens[pos].is_operator(Operator::Colon)
						{
							pos += 1;
							if pos < self.tokens.len() {
								pos += 1;
							}
						}
						continue;
					}
					return false;
				}

				return false;
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
	fn test_closure_simple() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "let $double = ($x) { $x * 2 }").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Let(let_node) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Let")
		};

		if let crate::ast::ast::LetValue::Expression(expr) = let_node.value {
			assert!(matches!(*expr, Ast::Closure(_)));
		} else {
			panic!("Expected expression value");
		}
	}

	#[test]
	fn test_closure_multi_param() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "let $add = ($a, $b) { $a + $b }").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Let(let_node) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Let")
		};

		if let crate::ast::ast::LetValue::Expression(expr) = let_node.value {
			let inner = crate::bump::BumpBox::into_inner(expr);
			if let Ast::Closure(closure) = inner {
				assert_eq!(closure.parameters.len(), 2);
				assert_eq!(closure.parameters[0].variable.name(), "a");
				assert_eq!(closure.parameters[1].variable.name(), "b");
			} else {
				panic!("Expected Closure");
			}
		} else {
			panic!("Expected expression value");
		}
	}

	#[test]
	fn test_closure_empty_params() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "let $greet = () { 42 }").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Let(let_node) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Let")
		};

		if let crate::ast::ast::LetValue::Expression(expr) = let_node.value {
			let inner = crate::bump::BumpBox::into_inner(expr);
			if let Ast::Closure(closure) = inner {
				assert_eq!(closure.parameters.len(), 0);
			} else {
				panic!("Expected Closure");
			}
		} else {
			panic!("Expected expression value");
		}
	}
}
