// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{Error, diagnostic::ast::unexpected_token_error};

use crate::ast::{
	Ast, AstList, TokenKind,
	ast::{AstFrom, AstGenerator},
	parse::Parser,
	tokenize::{
		Keyword, Operator,
		Operator::{CloseBracket, OpenBracket, OpenCurly},
		Separator,
	},
};

impl Parser {
	pub(crate) fn parse_from(&mut self) -> crate::Result<AstFrom> {
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
			use crate::ast::{AstVariable, identifier::UnresolvedPrimitiveIdentifier};

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
					return Err(Error(unexpected_token_error(
						"expected identifier or variable",
						current.fragment.clone(),
					)));
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
				let (nodes, _has_braces) = self.parse_expressions(true)?; // Parse { ... } content

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
				self.consume_operator(Operator::Dot)?;
				let second_identifier = self.parse_identifier_with_hyphens()?;

				// namespace.table - create
				// UnresolvedPrimitiveIdentifier with namespace
				let mut source = UnresolvedPrimitiveIdentifier::new(
					Some(first_identifier.fragment().clone()),
					second_identifier.into_fragment(),
				);

				// Check for alias after namespace.table
				if !self.is_eof() && self.current()?.is_identifier() {
					let alias_token = self.consume(TokenKind::Identifier)?;
					source = source.with_alias(alias_token.fragment.clone());
				}

				source
			} else {
				// Just table - create
				// UnresolvedPrimitiveIdentifier without namespace
				let mut source =
					UnresolvedPrimitiveIdentifier::new(None, first_identifier.into_fragment());

				// Check for alias after table
				if !self.is_eof() && self.current()?.is_identifier() {
					let alias_token = self.consume(TokenKind::Identifier)?;
					source = source.with_alias(alias_token.fragment.clone());
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

	pub(crate) fn parse_static(&mut self) -> crate::Result<AstList> {
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

			nodes.push(Ast::Inline(self.parse_inline()?));

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
mod tests {
	use crate::ast::{AstFrom, InfixOperator::TypeAscription, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_from_schema_and_table() {
		let tokens = tokenize("FROM reifydb.users").unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(source.namespace.as_ref().unwrap().text(), "reifydb");
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name, &None);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_table_without_schema() {
		let tokens = tokenize("FROM users").unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(source.namespace, None);
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name, &None);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_empty() {
		let tokens = tokenize("FROM []").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("FROM [ { field: 'value' }]").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize(
			"FROM [ { field: 'value' },\
        { field: 'value2' }\
        ]",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("FROM users::user_id_pk").unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(source.namespace, None);
				assert_eq!(source.name.text(), "users");
				assert_eq!(index_name.as_ref().unwrap().text(), "user_id_pk");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_namespace_table_with_index_directive() {
		let tokens = tokenize("FROM company.employees::employee_email_pk").unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(source.namespace.as_ref().unwrap().text(), "company");
				assert_eq!(source.name.text(), "employees");
				assert_eq!(index_name.as_ref().unwrap().text(), "employee_email_pk");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_table_with_alias() {
		let tokens = tokenize("FROM orders o").unwrap();
		let mut parser = Parser::new(tokens);
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
				assert!(source.namespace.is_none());
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(source.alias.as_ref().unwrap().text(), "o");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_namespace_table_with_alias() {
		let tokens = tokenize("FROM test.orders o").unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(source.name.text(), "orders");
				assert_eq!(index_name, &None);
				assert_eq!(source.alias.as_ref().unwrap().text(), "o");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_from_static_trailing_comma() {
		let tokens = tokenize("FROM [ { field: 'value' }, ]").unwrap();
		let mut parser = Parser::new(tokens);
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
	fn test_from_generator_simple() {
		let tokens = tokenize("FROM generate_series { start: 1, end: 100 }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Generator(generator) => {
				assert_eq!(generator.name.text(), "generate_series");
				assert_eq!(generator.nodes.len(), 2);

				let first_param = generator.nodes[0].as_infix();
				assert!(matches!(first_param.operator, crate::ast::InfixOperator::As(_)));
				assert_eq!(first_param.left.as_literal_number().value(), "1");
				assert_eq!(first_param.right.as_identifier().text(), "start");

				let second_param = generator.nodes[1].as_infix();
				assert!(matches!(second_param.operator, TypeAscription(_)));
				assert_eq!(second_param.left.as_identifier().text(), "end");
				assert_eq!(second_param.right.as_literal_number().value(), "100");
			}
			_ => unreachable!("Expected Generator"),
		}
	}

	#[test]
	fn test_from_generator_complex() {
		let tokens = tokenize("FROM data_loader { endpoint: '/api/v1', timeout: 30 * 1000 }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let from = result.first_unchecked().as_from();

		match from {
			AstFrom::Generator(generator) => {
				assert_eq!(generator.name.text(), "data_loader");
				assert_eq!(generator.nodes.len(), 2);

				let first_param = generator.nodes[0].as_infix();
				assert!(matches!(first_param.operator, crate::ast::InfixOperator::As(_)));
				assert_eq!(first_param.left.as_literal_text().value(), "/api/v1");
				assert_eq!(first_param.right.as_identifier().text(), "endpoint");

				let second_param = generator.nodes[1].as_infix();
				assert!(matches!(second_param.operator, crate::ast::InfixOperator::As(_)));

				let timeout_expr = second_param.left.as_infix();
				assert!(matches!(timeout_expr.operator, crate::ast::InfixOperator::Multiply(_)));
				assert_eq!(timeout_expr.left.as_literal_number().value(), "30");
				assert_eq!(timeout_expr.right.as_literal_number().value(), "1000");

				assert_eq!(second_param.right.as_identifier().text(), "timeout");
			}
			_ => unreachable!("Expected Generator"),
		}
	}

	#[test]
	fn test_from_table_with_hyphens() {
		// Test: FROM hyphenated-table
		let tokens = tokenize("FROM my-table").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_from().unwrap();

		// Should parse as single table identifier "my-table"
		if let AstFrom::Source {
			source,
			..
		} = result
		{
			assert_eq!(source.namespace, None);
			assert_eq!(source.name.text(), "my-table");
		} else {
			panic!("Expected AstFrom::Source");
		}
	}

	#[test]
	fn test_from_namespace_table_with_hyphens() {
		// Test: FROM namespace.hyphenated-table
		let tokens = tokenize("FROM test.even-numbers").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_from().unwrap();

		// Should parse namespace="test", table="even-numbers"
		if let AstFrom::Source {
			source,
			..
		} = result
		{
			assert_eq!(source.namespace.as_ref().map(|f| f.text()), Some("test"));
			assert_eq!(source.name.text(), "even-numbers");
		} else {
			panic!("Expected AstFrom::Source");
		}
	}

	#[test]
	fn test_from_hyphenated_with_alias() {
		// Test: FROM my-table AS t
		let tokens = tokenize("FROM my-table t").unwrap();
		let mut parser = Parser::new(tokens);
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
		// Test: FROM test.even-numbers nums
		let tokens = tokenize("FROM test.even-numbers nums").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_from().unwrap();

		if let AstFrom::Source {
			source,
			..
		} = result
		{
			assert_eq!(source.namespace.as_ref().map(|f| f.text()), Some("test"));
			assert_eq!(source.name.text(), "even-numbers");
			assert_eq!(source.alias.as_ref().map(|f| f.text()), Some("nums"));
		} else {
			panic!("Expected AstFrom::Source");
		}
	}
}
