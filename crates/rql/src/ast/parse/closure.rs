// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{AstClosure, AstFunctionParameter, AstVariable},
		parse::Parser,
	},
	token::{operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_closure(&mut self) -> Result<AstClosure<'bump>> {
		let token = self.consume_operator(Operator::OpenParen)?;

		let parameters = self.parse_closure_parameters()?;

		self.consume_operator(Operator::CloseParen)?;

		let body = self.parse_block()?;

		Ok(AstClosure {
			token,
			parameters,
			body,
		})
	}

	fn parse_closure_parameters(&mut self) -> Result<Vec<AstFunctionParameter<'bump>>> {
		let mut parameters = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseParen) {
				break;
			}

			let param_token = self.consume(TokenKind::Variable)?;
			let variable = AstVariable {
				token: param_token,
			};

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

			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseParen) {
				break;
			}
		}

		Ok(parameters)
	}

	pub(crate) fn is_closure_pattern(&self) -> bool {
		if self.position >= self.tokens.len() {
			return false;
		}

		if !self.tokens[self.position].is_operator(Operator::OpenParen) {
			return false;
		}

		let mut pos = self.position + 1;

		if pos < self.tokens.len() && self.tokens[pos].is_operator(Operator::CloseParen) {
			pos += 1;
			return pos < self.tokens.len() && self.tokens[pos].is_operator(Operator::OpenCurly);
		}

		if pos < self.tokens.len() && matches!(self.tokens[pos].kind, TokenKind::Variable) {
			pos += 1;

			if pos < self.tokens.len() && self.tokens[pos].is_operator(Operator::Colon) {
				pos += 1;

				if pos < self.tokens.len() {
					pos += 1;
				}
			}

			loop {
				if pos >= self.tokens.len() {
					return false;
				}

				if self.tokens[pos].is_operator(Operator::CloseParen) {
					pos += 1;
					return pos < self.tokens.len()
						&& self.tokens[pos].is_operator(Operator::OpenCurly);
				}

				if self.tokens[pos].is_separator(Separator::Comma) {
					pos += 1;

					if pos < self.tokens.len()
						&& matches!(self.tokens[pos].kind, TokenKind::Variable)
					{
						pos += 1;

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
		ast::{
			ast::{Ast, LetValue},
			parse::parse,
		},
		bump::{Bump, BumpBox},
		token::tokenize,
	};

	#[test]
	fn test_closure_simple() {
		let bump = Bump::new();
		let source = "let $double = ($x) { $x * 2 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Let(let_node) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Let")
		};

		if let LetValue::Expression(expr) = let_node.value {
			assert!(matches!(*expr, Ast::Closure(_)));
		} else {
			panic!("Expected expression value");
		}
	}

	#[test]
	fn test_closure_multi_param() {
		let bump = Bump::new();
		let source = "let $add = ($a, $b) { $a + $b }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Let(let_node) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Let")
		};

		if let LetValue::Expression(expr) = let_node.value {
			let inner = BumpBox::into_inner(expr);
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
		let source = "let $greet = () { 42 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Let(let_node) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Let")
		};

		if let LetValue::Expression(expr) = let_node.value {
			let inner = BumpBox::into_inner(expr);
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
