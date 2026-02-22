// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::AstAggregate,
		parse::{Parser, Precedence},
	},
	error::{OperationKind, RqlError},
	token::{
		keyword::Keyword,
		operator::Operator::{CloseCurly, OpenCurly},
		separator::Separator::Comma,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_aggregate(&mut self) -> crate::Result<AstAggregate<'bump>> {
		let token = self.consume_keyword(Keyword::Aggregate)?;

		let mut projections = Vec::new();

		if !self.current()?.is_keyword(Keyword::By) {
			if !self.current()?.is_operator(OpenCurly) {
				return Err(RqlError::OperatorMissingBraces {
					kind: OperationKind::Aggregate,
					fragment: token.fragment.to_owned(),
				}
				.into());
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
			return Err(RqlError::OperatorMissingBraces {
				kind: OperationKind::AggregateBy,
				fragment: by_token.fragment.to_owned(),
			}
			.into());
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
	use crate::{
		ast::ast::{Ast, InfixOperator},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_single_column() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE {min(age)} BY {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE {min(value)} BY {value}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "AGGREGATE { min_age: min(age) } BY {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE BY {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE BY {name, age}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE {min(age), max(age)} BY {name, gender}")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE {min(age)} BY {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE BY {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 0);
		assert_eq!(aggregate.by.len(), 1);
		assert_eq!(aggregate.by[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_maps_without_braces_fails() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE min(age) BY {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "AGGREGATE_004")
	}

	#[test]
	fn test_by_without_braces_fails() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE { count(value) } BY name").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "AGGREGATE_005")
	}

	#[test]
	fn test_empty_by_clause() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE { count(value) } BY {}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "AGGREGATE { count(value) } ").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
