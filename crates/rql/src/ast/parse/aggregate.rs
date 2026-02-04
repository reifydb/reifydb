// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::{aggregate_by_missing_braces, aggregate_missing_braces};
use reifydb_type::return_error;

use crate::ast::{
	ast::AstAggregate,
	parse::{Parser, Precedence},
	tokenize::{
		keyword::Keyword,
		operator::Operator::{CloseCurly, OpenCurly},
		separator::Separator::Comma,
	},
};

impl Parser {
	pub(crate) fn parse_aggregate(&mut self) -> crate::Result<AstAggregate> {
		let token = self.consume_keyword(Keyword::Aggregate)?;

		let mut projections = Vec::new();

		if !self.current()?.is_keyword(Keyword::By) {
			if !self.current()?.is_operator(OpenCurly) {
				return_error!(aggregate_missing_braces(token.fragment));
			}

			self.advance()?;

			if !self.current()?.is_operator(CloseCurly) {
				loop {
					if self.current()?.is_keyword(Keyword::By) {
						break;
					}

					// Try colon alias syntax first (alias: expr), otherwise parse with LogicOr
					// precedence to prevent AS keyword from being parsed
					if let Ok(alias_expr) = self.try_parse_colon_alias() {
						projections.push(alias_expr);
					} else {
						projections.push(self.parse_node(Precedence::LogicOr)?);
					}

					if self.is_eof() {
						break;
					}

					if self.current()?.is_operator(CloseCurly) {
						self.advance()?;
						break;
					}

					if self.current()?.is_separator(Comma) {
						self.advance()?;
					} else {
						break;
					}
				}
			} else {
				self.advance()?;
			}
		}

		let has_by_keyword = self.current().map_or(false, |t| t.is_keyword(Keyword::By));

		if !has_by_keyword {
			return Ok(AstAggregate {
				token,
				by: Vec::new(),
				map: projections,
			});
		}

		let by_token = self.consume_keyword(Keyword::By)?;

		if !self.current()?.is_operator(OpenCurly) {
			return_error!(aggregate_by_missing_braces(by_token.fragment));
		}

		self.advance()?;

		let mut by = Vec::new();

		if !self.current()?.is_operator(CloseCurly) {
			loop {
				// Use LogicOr precedence to prevent AS keyword from being parsed
				by.push(self.parse_node(Precedence::LogicOr)?);

				if self.is_eof() {
					break;
				}

				if self.current()?.is_operator(CloseCurly) {
					self.advance()?;
					break;
				}

				if self.current()?.is_separator(Comma) {
					self.advance()?;
				} else {
					break;
				}
			}
		} else {
			self.advance()?;
		}

		Ok(AstAggregate {
			token,
			by,
			map: projections,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::ast::{
		ast::{Ast, InfixOperator},
		tokenize::tokenize,
	};

	#[test]
	fn test_single_column() {
		let tokens = tokenize("AGGREGATE {min(age)} BY {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_call_function();
		assert_eq!(projection.function.name.text(), "min");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "age");

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_keyword() {
		let tokens = tokenize("AGGREGATE {min(value)} BY {value}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_call_function();
		assert_eq!(projection.function.name.text(), "min");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "value");

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].as_identifier().text(), "value");
	}

	#[test]
	fn test_alias_colon() {
		let tokens = tokenize("AGGREGATE { min_age: min(age) } BY {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_infix();

		// Colon syntax is converted to AS operator internally: expr AS alias
		let min_call = projection.left.as_call_function();
		assert_eq!(min_call.function.name.text(), "min");
		assert!(min_call.function.namespaces.is_empty());

		assert_eq!(min_call.arguments.len(), 1);
		let identifier = min_call.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "age");

		assert!(matches!(projection.operator, InfixOperator::As(_)));
		let identifier = projection.right.as_identifier();
		assert_eq!(identifier.text(), "min_age");

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_no_projection_single_column() {
		let tokens = tokenize("AGGREGATE BY {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 0);

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_no_projection_multiple_columns() {
		let tokens = tokenize("AGGREGATE BY {name, age}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 0);
		assert_eq!(aggregate.by.len(), 2);

		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");

		assert!(matches!(aggregate.by[1], Ast::Identifier(_)));
		assert_eq!(aggregate.by[1].as_identifier().text(), "age");
	}

	#[test]
	fn test_many() {
		let tokens = tokenize("AGGREGATE {min(age), max(age)} BY {name, gender}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 2);

		let projection = &aggregate.map[0].as_call_function();
		assert_eq!(projection.function.name.text(), "min");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "age");

		let projection = &aggregate.map[1].as_call_function();
		assert_eq!(projection.function.name.text(), "max");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "age");

		assert_eq!(aggregate.by.len(), 2);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");

		assert!(matches!(aggregate.by[1], Ast::Identifier(_)));
		assert_eq!(aggregate.by[1].as_identifier().text(), "gender");
	}

	#[test]
	fn test_single_projection_with_braces() {
		let tokens = tokenize("AGGREGATE {min(age)} BY {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_call_function();
		assert_eq!(projection.function.name.text(), "min");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "age");

		assert_eq!(aggregate.by.len(), 1);
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_single_by_with_braces() {
		let tokens = tokenize("AGGREGATE BY {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 0);
		assert_eq!(aggregate.by.len(), 1);
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_maps_without_braces_fails() {
		let tokens = tokenize("AGGREGATE min(age) BY {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "AGGREGATE_004")
	}

	#[test]
	fn test_by_without_braces_fails() {
		let tokens = tokenize("AGGREGATE { count(value) } BY name").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "AGGREGATE_005")
	}

	#[test]
	fn test_empty_by_clause() {
		let tokens = tokenize("AGGREGATE { count(value) } BY {}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_call_function();
		assert_eq!(projection.function.name.text(), "count");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "value");

		assert_eq!(aggregate.by.len(), 0);
	}

	#[test]
	fn test_global_aggregate() {
		let tokens = tokenize("AGGREGATE { count(value) } ").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_call_function();
		assert_eq!(projection.function.name.text(), "count");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.text(), "value");

		assert_eq!(aggregate.by.len(), 0);
	}
}
