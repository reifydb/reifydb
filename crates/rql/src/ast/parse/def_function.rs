// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstDefFunction, AstFunctionParameter, AstReturn, AstType, AstVariable},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse `fun name ($param: type, ...) : return_type { body }`
	pub(crate) fn parse_def_function(&mut self) -> crate::Result<AstDefFunction<'bump>> {
		let token = self.consume_keyword(Keyword::Fun)?;
		let name = self.parse_as_identifier()?;

		// Parse parameters: ($a: type, $b: type)
		self.consume_operator(Operator::OpenParen)?;
		let parameters = self.parse_function_parameters()?;
		self.consume_operator(Operator::CloseParen)?;

		// Optional return type: : type
		let return_type = if !self.is_eof() && self.current()?.is_operator(Operator::Colon) {
			self.advance()?;
			Some(self.parse_type_annotation()?)
		} else {
			None
		};

		let body = self.parse_block()?;

		Ok(AstDefFunction {
			token,
			name,
			parameters,
			return_type,
			body,
		})
	}

	/// Parse function parameters: $var: type, $var2: type
	fn parse_function_parameters(&mut self) -> crate::Result<Vec<AstFunctionParameter<'bump>>> {
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

	/// Parse a type annotation (identifier with optional parameters)
	pub(crate) fn parse_type_annotation(&mut self) -> crate::Result<AstType<'bump>> {
		let ty_token = self.consume(TokenKind::Identifier)?;

		// Check for Option(T) syntax
		if ty_token.fragment.text().eq_ignore_ascii_case("option") {
			self.consume_operator(Operator::OpenParen)?;
			let inner = self.parse_type_annotation()?;
			self.consume_operator(Operator::CloseParen)?;
			return Ok(AstType::Optional(Box::new(inner)));
		}

		// Check for type with parameters like DECIMAL(10,2)
		if !self.is_eof() && self.current()?.is_operator(Operator::OpenParen) {
			self.consume_operator(Operator::OpenParen)?;
			let mut params = Vec::new();

			// Parse first parameter
			params.push(self.parse_literal_number()?);

			// Parse additional parameters if comma-separated
			while self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				params.push(self.parse_literal_number()?);
			}

			self.consume_operator(Operator::CloseParen)?;

			Ok(AstType::Constrained {
				name: ty_token.fragment,
				params,
			})
		} else {
			Ok(AstType::Unconstrained(ty_token.fragment))
		}
	}

	/// Parse `RETURN` or `RETURN expr`
	pub(crate) fn parse_return(&mut self) -> crate::Result<AstReturn<'bump>> {
		let token = self.consume_keyword(Keyword::Return)?;

		// Check if there's a value to return (not at EOF, semicolon, or closing brace)
		let value = if !self.is_eof() {
			let current = self.current()?;
			if current.is_separator(Separator::Semicolon)
				|| current.is_separator(Separator::NewLine)
				|| current.is_operator(Operator::CloseCurly)
			{
				None
			} else {
				Some(BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump()))
			}
		} else {
			None
		};

		Ok(AstReturn {
			token,
			value,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, AstType},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_def_function_no_params() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FUN hello () { MAP { \"message\": \"Hello\" } }")
			.unwrap()
			.into_iter()
			.collect();
		let mut result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::DefFunction(def) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected DefFunction")
		};

		assert_eq!(def.name.text(), "hello");
		assert!(def.parameters.is_empty());
		assert!(def.return_type.is_none());
	}

	#[test]
	fn test_def_function_with_params() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FUN greet ($name) { MAP { \"message\": $name } }")
			.unwrap()
			.into_iter()
			.collect();
		let mut result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::DefFunction(def) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected DefFunction")
		};

		assert_eq!(def.name.text(), "greet");
		assert_eq!(def.parameters.len(), 1);
		assert_eq!(def.parameters[0].variable.name(), "name");
		assert!(def.parameters[0].type_annotation.is_none());
	}

	#[test]
	fn test_def_function_with_typed_params() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FUN add ($a: int, $b: int) { $a + $b }").unwrap().into_iter().collect();
		let mut result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::DefFunction(def) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected DefFunction")
		};

		assert_eq!(def.name.text(), "add");
		assert_eq!(def.parameters.len(), 2);

		assert_eq!(def.parameters[0].variable.name(), "a");
		match &def.parameters[0].type_annotation {
			Some(AstType::Unconstrained(ty)) => assert_eq!(ty.text(), "int"),
			_ => panic!("Expected unconstrained type"),
		}

		assert_eq!(def.parameters[1].variable.name(), "b");
		match &def.parameters[1].type_annotation {
			Some(AstType::Unconstrained(ty)) => assert_eq!(ty.text(), "int"),
			_ => panic!("Expected unconstrained type"),
		}
	}

	#[test]
	fn test_def_function_with_return_type() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "FUN add ($a: int, $b: int) : int { $a + $b }").unwrap().into_iter().collect();
		let mut result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::DefFunction(def) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected DefFunction")
		};

		assert_eq!(def.name.text(), "add");
		match &def.return_type {
			Some(AstType::Unconstrained(ty)) => assert_eq!(ty.text(), "int"),
			_ => panic!("Expected unconstrained return type"),
		}
	}

	#[test]
	fn test_def_function_mixed_typed_params() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FUN example ($x, $y: int) { $x + $y }").unwrap().into_iter().collect();
		let mut result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::DefFunction(def) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected DefFunction")
		};

		assert_eq!(def.name.text(), "example");
		assert_eq!(def.parameters.len(), 2);

		assert_eq!(def.parameters[0].variable.name(), "x");
		assert!(def.parameters[0].type_annotation.is_none());

		assert_eq!(def.parameters[1].variable.name(), "y");
		assert!(def.parameters[1].type_annotation.is_some());
	}

	#[test]
	fn test_return_with_value() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "RETURN 42").unwrap().into_iter().collect();
		let mut result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Return(ret) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Return")
		};

		assert!(ret.value.is_some());
	}

	#[test]
	fn test_return_without_value() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "RETURN;").unwrap().into_iter().collect();
		let mut result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Return(ret) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!("Expected Return")
		};

		assert!(ret.value.is_none());
	}
}
