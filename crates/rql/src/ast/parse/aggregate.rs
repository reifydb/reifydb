// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{
	diagnostic::operation::{
		aggregate_multiple_by_without_braces,
		aggregate_multiple_map_without_braces,
	},
	return_error,
};

use crate::ast::{
	AstAggregate,
	parse::{Parser, Precedence},
	tokenize::{
		Keyword,
		Operator::{CloseCurly, OpenCurly},
		Separator::Comma,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_aggregate(
		&mut self,
	) -> crate::Result<AstAggregate<'a>> {
		let token = self.consume_keyword(Keyword::Aggregate)?;

		let mut projections = Vec::new();

		if !self.current()?.is_keyword(Keyword::By) {
			let has_projections_braces =
				self.current()?.is_operator(OpenCurly);

			if has_projections_braces {
				self.advance()?; // consume opening brace
			}

			loop {
				if self.current()?.is_keyword(Keyword::By) {
					break;
				}

				projections.push(
					self.parse_node(Precedence::None)?
				);

				if self.is_eof() {
					break;
				}

				// If we have braces, look for closing brace
				if has_projections_braces
					&& self.current()?
						.is_operator(CloseCurly)
				{
					self.advance()?; // consume closing brace
					break;
				}

				if self.current()?.is_separator(Comma) {
					self.advance()?;
				} else {
					break;
				}
			}

			if projections.len() > 1 && !has_projections_braces {
				return_error!(
					aggregate_multiple_map_without_braces(
						token.fragment
					)
				);
			}
		}

		// Note: We allow empty projections for group-by-only operations
		// This can be useful for getting distinct groups without
		// aggregations

		// BY clause is optional - if not present, it's a global
		// aggregation
		let has_by_keyword = self
			.current()
			.map_or(false, |t| t.is_keyword(Keyword::By));

		if !has_by_keyword {
			// No BY clause means global aggregation with empty
			// grouping
			return Ok(AstAggregate {
				token,
				by: Vec::new(),
				map: projections,
			});
		}

		let _ = self.consume_keyword(Keyword::By)?;

		let has_by_braces = self.current()?.is_operator(OpenCurly);

		if has_by_braces {
			self.advance()?; // consume opening brace
		}

		let mut by = Vec::new();

		// Check for empty braces first
		if has_by_braces && self.current()?.is_operator(CloseCurly) {
			self.advance()?; // consume closing brace
		// Empty by clause for global aggregation
		} else {
			loop {
				by.push(self.parse_node(Precedence::None)?);

				if self.is_eof() {
					break;
				}

				// If we have braces, look for closing brace
				if has_by_braces
					&& self.current()?
						.is_operator(CloseCurly)
				{
					self.advance()?; // consume closing brace
					break;
				}

				if self.current()?.is_separator(Comma) {
					self.advance()?;
				} else {
					break;
				}
			}
		}

		if by.len() > 1 && !has_by_braces {
			return_error!(aggregate_multiple_by_without_braces(
				token.fragment
			));
		}

		Ok(AstAggregate {
			token,
			by,
			map: projections,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::{Ast, InfixOperator, tokenize::tokenize};

	#[test]
	fn test_single_column() {
		let tokens = tokenize("AGGREGATE min(age) BY name").unwrap();
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
		assert_eq!(identifier.value(), "age");

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].value(), "name");
	}

	#[test]
	fn test_keyword() {
		let tokens = tokenize("AGGREGATE min(value) BY value").unwrap();
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
		assert_eq!(identifier.value(), "value");

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].value(), "value");
	}

	#[test]
	fn test_alias() {
		let tokens = tokenize("AGGREGATE min(age) as min_age BY name")
			.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_infix();

		let min_call = projection.left.as_call_function();
		assert_eq!(min_call.function.name.text(), "min");
		assert!(min_call.function.namespaces.is_empty());

		assert_eq!(min_call.arguments.len(), 1);
		let identifier = min_call.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.value(), "age");

		assert!(matches!(projection.operator, InfixOperator::As(_)));
		let identifier = projection.right.as_identifier();
		assert_eq!(identifier.value(), "min_age");

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].value(), "name");
	}

	#[test]
	fn test_alias_colon() {
		let tokens =
			tokenize("AGGREGATE { min_age: min(age) } BY name")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 1);

		let projection = &aggregate.map[0].as_infix();

		let identifier = projection.left.as_identifier();
		assert_eq!(identifier.value(), "min_age");

		assert!(matches!(
			projection.operator,
			InfixOperator::TypeAscription(_)
		));

		let min_call = projection.right.as_call_function();
		assert_eq!(min_call.function.name.text(), "min");
		assert!(min_call.function.namespaces.is_empty());

		assert_eq!(min_call.arguments.len(), 1);
		let identifier = min_call.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.value(), "age");

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].value(), "name");
	}

	#[test]
	fn test_no_projection_single_column() {
		let tokens = tokenize("AGGREGATE BY name").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let aggregate = result.first_unchecked().as_aggregate();
		assert_eq!(aggregate.map.len(), 0);

		assert_eq!(aggregate.by.len(), 1);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].value(), "name");
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
		assert_eq!(aggregate.by[0].value(), "name");

		assert!(matches!(aggregate.by[1], Ast::Identifier(_)));
		assert_eq!(aggregate.by[1].value(), "age");
	}

	#[test]
	fn test_many() {
		let tokens = tokenize(
			"AGGREGATE {min(age), max(age)} BY {name, gender}",
		)
		.unwrap();
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
		assert_eq!(identifier.value(), "age");

		let projection = &aggregate.map[1].as_call_function();
		assert_eq!(projection.function.name.text(), "max");
		assert!(projection.function.namespaces.is_empty());

		assert_eq!(projection.arguments.len(), 1);
		let identifier = projection.arguments.nodes[0].as_identifier();
		assert_eq!(identifier.value(), "age");

		assert_eq!(aggregate.by.len(), 2);
		assert!(matches!(aggregate.by[0], Ast::Identifier(_)));
		assert_eq!(aggregate.by[0].value(), "name");

		assert!(matches!(aggregate.by[1], Ast::Identifier(_)));
		assert_eq!(aggregate.by[1].value(), "gender");
	}

	#[test]
	fn test_single_projection_with_braces() {
		let tokens = tokenize("AGGREGATE {min(age)} BY name").unwrap();
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
		assert_eq!(identifier.value(), "age");

		assert_eq!(aggregate.by.len(), 1);
		assert_eq!(aggregate.by[0].value(), "name");
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
		assert_eq!(aggregate.by[0].value(), "name");
	}

	#[test]
	fn test_multiple_maps_without_braces_fails() {
		let tokens = tokenize("AGGREGATE min(age), max(age) BY name")
			.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "AGGREGATE_002")
	}

	#[test]
	fn test_multiple_by_without_braces_fails() {
		let tokens =
			tokenize("AGGREGATE { count(value) } BY name, age")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "AGGREGATE_003")
	}

	#[test]
	fn test_empty_by_clause() {
		let tokens =
			tokenize("AGGREGATE { count(value) } BY {}").unwrap();
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
		assert_eq!(identifier.value(), "value");

		assert_eq!(
			aggregate.by.len(),
			0,
			"BY clause should be empty for global aggregation"
		);
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
		assert_eq!(identifier.value(), "value");

		assert_eq!(
			aggregate.by.len(),
			0,
			"BY clause should be empty for global aggregation"
		);
	}
}
