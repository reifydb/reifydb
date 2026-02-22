// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{Ast, AstFrom, AstGenerator, AstList, AstVariable},
		identifier::UnresolvedPrimitiveIdentifier,
		parse::Parser,
	},
	diagnostic::AstError,
	token::{
		keyword::Keyword,
		operator::{
			Operator,
			Operator::{CloseBracket, OpenBracket, OpenCurly, OpenParen},
		},
		separator::Separator,
		token::TokenKind,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_from(&mut self) -> crate::Result<AstFrom<'bump>> {
		let token = self.consume_keyword(Keyword::From)?;

		// Check token type first
		let is_inline = if let Ok(current) = self.current() {
			current.is_operator(OpenBracket)
		} else {
			false
		};

		if is_inline {
			Ok(AstFrom::Inline {
				token,
				list: self.parse_static()?,
			})
		} else {
			// Check if this is a variable or identifier
			let current = self.current()?;
			match current.kind {
				TokenKind::Variable => {
					let var_token = self.advance()?;

					if var_token.fragment.text() == "$env" {
						return Ok(AstFrom::Environment {
							token,
						});
					}

					let variable = AstVariable {
						token: var_token,
					};

					return Ok(AstFrom::Variable {
						token,
						variable,
					});
				}
				TokenKind::Identifier | TokenKind::Keyword(_) => {}
				_ => {
					return Err(AstError::UnexpectedToken {
						expected: "expected identifier or variable".to_string(),
						fragment: current.fragment.to_owned(),
					}
					.into());
				}
			}

			// Get the first identifier (with hyphen support)
			let first_identifier = self.parse_identifier_with_hyphens()?;

			// Check if this is a generator function call: identifier { ... }
			let is_generatortion = if !self.is_eof() {
				if let Ok(current) = self.current() {
					current.is_operator(OpenCurly)
				} else {
					false
				}
			} else {
				false
			};

			if is_generatortion {
				// Parse as generator function
				let function_name = first_identifier;
				let (nodes, _has_braces) = self.parse_expressions(true, false)?;

				return Ok(AstFrom::Generator(AstGenerator {
					token,
					name: function_name.into_fragment(),
					nodes,
				}));
			}

			// Check if there's a dot following
			let has_dot = if !self.is_eof() {
				if let Ok(current) = self.current() {
					current.is_operator(Operator::Dot)
				} else {
					false
				}
			} else {
				false
			};

			let source = if has_dot {
				let mut segments = vec![first_identifier];
				while !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
					self.consume_operator(Operator::Dot)?;
					segments.push(self.parse_identifier_with_hyphens()?);
				}
				let name = segments.pop().unwrap().into_fragment();
				let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();

				let mut source = UnresolvedPrimitiveIdentifier::new(namespace, name);

				if !self.is_eof() && self.current()?.is_identifier() {
					let alias_token = self.consume(TokenKind::Identifier)?;
					source = source.with_alias(alias_token.fragment);
				}

				source
			} else {
				let mut source =
					UnresolvedPrimitiveIdentifier::new(vec![], first_identifier.into_fragment());

				if !self.is_eof() && self.current()?.is_identifier() {
					let alias_token = self.consume(TokenKind::Identifier)?;
					source = source.with_alias(alias_token.fragment);
				}

				source
			};

			// Check for index directive using ::
			let index_name = if !self.is_eof() {
				if let Ok(current) = self.current() {
					if current.is_operator(Operator::DoubleColon) {
						self.consume_operator(Operator::DoubleColon)?;
						let index_token = self.consume(TokenKind::Identifier)?;
						Some(index_token.fragment)
					} else {
						None
					}
				} else {
					None
				}
			} else {
				None
			};

			Ok(AstFrom::Source {
				token,
				source,
				index_name,
			})
		}
	}

	pub(crate) fn parse_static(&mut self) -> crate::Result<AstList<'bump>> {
		let token = self.consume_operator(OpenBracket)?;

		let mut nodes = Vec::new();
		loop {
			self.skip_new_line()?;

			// Check if we've reached the closing bracket
			let should_break = if let Ok(current) = self.current() {
				current.is_operator(CloseBracket)
			} else {
				true
			};

			if should_break {
				break;
			}

			let current = self.current()?;
			if current.is_operator(OpenParen) {
				nodes.push(Ast::Tuple(self.parse_tuple()?));
			} else if current.is_operator(OpenCurly) {
				nodes.push(Ast::Inline(self.parse_inline()?));
			} else {
				return Err(AstError::UnexpectedToken {
					expected: "expected '{' or '(' in inline data".to_string(),
					fragment: current.fragment.to_owned(),
				}
				.into());
			}

			self.consume_if(TokenKind::Separator(Separator::Comma))?;
		}

		self.consume_operator(CloseBracket)?;
		Ok(AstList {
			token,
			nodes,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{AstFrom, InfixOperator::As},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_from_schema_and_table() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM reifydb.users").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(source.namespace[0].text(), "reifydb");
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name, &None);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_table_without_schema() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM users").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert!(source.namespace.is_empty());
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name, &None);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_empty() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM []").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Inline {
				list: query,
				..
			} => {
				let block = query;
				assert_eq!(block.len(), 0);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_static() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM [ { field: 'value' }]").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Inline {
				list,
				..
			} => {
				assert_eq!(list.len(), 1);

				let row = list[0].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_multiple() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			"FROM [ { field: 'value' },\
        { field: 'value2' }\
        ]",
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Inline {
				list,
				..
			} => {
				assert_eq!(list.len(), 2);

				let row = list[0].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value");

				let row = list[1].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value2");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_with_index_directive() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM users::user_id_pk").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert!(source.namespace.is_empty());
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name.as_ref().unwrap().text(), "user_id_pk");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_namespace_table_with_index_directive() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "FROM company.employees::employee_email_pk").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(source.namespace[0].text(), "company");
				assert_eq!(source.name.text(), "employees");
				assert_eq!(index_name.as_ref().unwrap().text(), "employee_email_pk");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_table_with_alias() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM orders o").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert!(source.namespace.is_empty());
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(source.alias.as_ref().unwrap().text(), "o");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_namespace_table_with_alias() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM test.orders o").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				assert_eq!(source.namespace[0].text(), "test");
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(source.alias.as_ref().unwrap().text(), "o");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_trailing_comma() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM [ { field: 'value' }, ]").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Inline {
				list,
				..
			} => {
				assert_eq!(list.len(), 1);

				let row = list[0].as_inline();
				assert_eq!(row.keyed_values.len(), 1);

				assert_eq!(row.keyed_values[0].key.text(), "field");
				assert_eq!(row.keyed_values[0].value.as_literal_text().value(), "value");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_parse_static_with_tuples() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM [(1, \"a\"), (2, \"b\")]").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Inline {
				list,
				..
			} => {
				assert_eq!(list.len(), 2);

				let tuple0 = list[0].as_tuple();
				assert_eq!(tuple0.len(), 2);

				let tuple1 = list[1].as_tuple();
				assert_eq!(tuple1.len(), 2);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_generator_simple() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "FROM generate_series { start: 1, end: 100 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Generator(generator) => {
				assert_eq!(generator.name.text(), "generate_series");
				assert_eq!(generator.nodes.len(), 2);

				let first_param = generator.nodes[0].as_infix();
				assert!(matches!(first_param.operator, As(_)));
				assert_eq!(first_param.left.as_literal_number().value(), "1");
				assert_eq!(first_param.right.as_identifier().text(), "start");

				let second_param = generator.nodes[1].as_infix();
				assert!(matches!(second_param.operator, As(_)));
				assert_eq!(second_param.left.as_literal_number().value(), "100");
				assert_eq!(second_param.right.as_identifier().text(), "end");
			}
			_ => unreachable!("Expected Generator"),
		}
	}

	#[test]
	fn test_from_generator_complex() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM data_loader { endpoint: '/api/v1', timeout: 30 * 1000 }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Generator(generator) => {
				assert_eq!(generator.name.text(), "data_loader");
				assert_eq!(generator.nodes.len(), 2);

				let first_param = generator.nodes[0].as_infix();
				assert!(matches!(first_param.operator, crate::ast::ast::InfixOperator::As(_)));
				assert_eq!(first_param.left.as_literal_text().value(), "/api/v1");
				assert_eq!(first_param.right.as_identifier().text(), "endpoint");

				let second_param = generator.nodes[1].as_infix();
				assert!(matches!(second_param.operator, crate::ast::ast::InfixOperator::As(_)));

				let timeout_expr = second_param.left.as_infix();
				assert!(matches!(timeout_expr.operator, crate::ast::ast::InfixOperator::Multiply(_)));
				assert_eq!(timeout_expr.left.as_literal_number().value(), "30");
				assert_eq!(timeout_expr.right.as_literal_number().value(), "1000");

				assert_eq!(second_param.right.as_identifier().text(), "timeout");
			}
			_ => unreachable!("Expected Generator"),
		}
	}

	#[test]
	fn test_from_table_with_hyphens() {
		let bump = Bump::new();
		// Test: FROM hyphenated-table
		let tokens = tokenize(&bump, "FROM my-table").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_from().unwrap();

		// Should parse as single table identifier "my-table"
		if let AstFrom::Source {
			source,
			..
		} = result
		{
			assert!(source.namespace.is_empty());
			assert_eq!(source.name.text(), "my-table");
		} else {
			panic!("Expected AstFrom::Source");
		}
	}

	#[test]
	fn test_from_namespace_table_with_hyphens() {
		let bump = Bump::new();
		// Test: FROM namespace.hyphenated-table
		let tokens = tokenize(&bump, "FROM test.even-numbers").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_from().unwrap();

		// Should parse namespace="test", table="even-numbers"
		if let AstFrom::Source {
			source,
			..
		} = result
		{
			assert_eq!(source.namespace.first().map(|f| f.text()), Some("test"));
			assert_eq!(source.name.text(), "even-numbers");
		} else {
			panic!("Expected AstFrom::Source");
		}
	}

	#[test]
	fn test_from_hyphenated_with_alias() {
		let bump = Bump::new();
		// Test: FROM my-table AS t
		let tokens = tokenize(&bump, "FROM my-table t").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_from().unwrap();

		// Should parse table="my-table", alias="t"
		if let AstFrom::Source {
			source,
			..
		} = result
		{
			assert_eq!(source.name.text(), "my-table");
			assert_eq!(source.alias.as_ref().map(|f| f.text()), Some("t"));
		} else {
			panic!("Expected AstFrom::Source");
		}
	}

	#[test]
	fn test_from_namespace_hyphens_with_alias() {
		let bump = Bump::new();
		// Test: FROM test.even-numbers nums
		let tokens = tokenize(&bump, "FROM test.even-numbers nums").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_from().unwrap();

		if let AstFrom::Source {
			source,
			..
		} = result
		{
			assert_eq!(source.namespace.first().map(|f| f.text()), Some("test"));
			assert_eq!(source.name.text(), "even-numbers");
			assert_eq!(source.alias.as_ref().map(|f| f.text()), Some("nums"));
		} else {
			panic!("Expected AstFrom::Source");
		}
	}
}
