// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::JoinType;
use reifydb_type::{diagnostic::ast, return_error};

use crate::ast::{
	AstJoin, AstJoinExpressionPair, AstUsingClause, JoinConnector,
	parse::{Parser, Precedence},
	tokenize::{
		Keyword::{Inner, Join, Left, Natural, Using},
		Operator::{And, As, CloseParen, OpenParen, Or},
		Separator::Comma,
		TokenKind,
	},
};

impl Parser {
	pub(crate) fn parse_join(&mut self) -> crate::Result<AstJoin> {
		let token = self.consume_keyword(Join)?;
		let with = self.parse_sub_query()?;

		// as <alias>
		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;

		// using (col1, col2) and ...
		let using_clause = self.parse_using_clause()?;

		Ok(AstJoin::InnerJoin {
			token,
			with,
			using_clause,
			alias,
		})
	}

	pub(crate) fn parse_natural_join(&mut self) -> crate::Result<AstJoin> {
		let token = self.consume_keyword(Natural)?;

		let join_type = if self.current()?.is_keyword(Left) {
			self.advance()?;
			Some(JoinType::Left)
		} else if self.current()?.is_keyword(Inner) {
			self.advance()?;
			Some(JoinType::Inner)
		} else {
			None
		};

		self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		// Required: as <alias>
		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;

		Ok(AstJoin::NaturalJoin {
			token,
			with,
			join_type,
			alias,
		})
	}

	pub(crate) fn parse_inner_join(&mut self) -> crate::Result<AstJoin> {
		let token = self.consume_keyword(Inner)?;
		self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		// as <alias>
		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;

		// using (col1, col2) and ...
		let using_clause = self.parse_using_clause()?;

		Ok(AstJoin::InnerJoin {
			token,
			with,
			using_clause,
			alias,
		})
	}

	pub(crate) fn parse_left_join(&mut self) -> crate::Result<AstJoin> {
		let token = self.consume_keyword(Left)?;
		self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		// as <alias>
		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;

		// using (col1, col2) and ...
		let using_clause = self.parse_using_clause()?;

		Ok(AstJoin::LeftJoin {
			token,
			with,
			using_clause,
			alias,
		})
	}

	/// Parse: using (expr, expr) and|or (expr, expr) ...
	fn parse_using_clause(&mut self) -> crate::Result<AstUsingClause> {
		let using_token = self.consume_keyword(Using)?;
		let mut pairs = Vec::new();

		loop {
			// Expect: (expression, expression)
			self.consume_operator(OpenParen)?;
			let first = self.parse_node(Precedence::None)?;
			// Consume comma separator
			if !self.current()?.is_separator(Comma) {
				return_error!(ast::tokenize_error("expected ','".to_string()));
			}
			self.advance()?;
			let second = self.parse_node(Precedence::None)?;
			self.consume_operator(CloseParen)?;

			// Check for connector ('and' or 'or')
			let connector = if !self.is_eof() {
				if self.current()?.is_operator(And) {
					self.advance()?;
					Some(JoinConnector::And)
				} else if self.current()?.is_operator(Or) {
					self.advance()?;
					Some(JoinConnector::Or)
				} else {
					None
				}
			} else {
				None
			};

			let has_more = connector.is_some();
			pairs.push(AstJoinExpressionPair {
				first: Box::new(first),
				second: Box::new(second),
				connector,
			});

			if !has_more {
				break;
			}
		}

		Ok(AstUsingClause {
			token: using_token,
			pairs,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::JoinType;

	use crate::ast::{Ast, AstFrom, AstJoin, AstLiteral, InfixOperator, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_left_join_with_using() {
		let tokens =
			tokenize("left join { from namespace.orders } as orders using (id, orders.user_id)").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::LeftJoin {
			with,
			using_clause,
			alias,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		// Check alias
		assert_eq!(alias.text(), "orders");

		// Check that the subquery contains "from namespace.orders"
		let first_node = with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.namespace.as_ref().unwrap().text(), "namespace");
			assert_eq!(source.name.text(), "orders");
		} else {
			panic!("Expected From node in subquery");
		}

		// Check using clause has one pair
		assert_eq!(using_clause.pairs.len(), 1);

		// Check first expression: id (unqualified - refers to current dataframe)
		let first = &using_clause.pairs[0].first;
		assert_eq!(first.as_identifier().text(), "id");

		// Check second expression: orders.user_id (qualified with join alias)
		let second = using_clause.pairs[0].second.as_infix();
		assert_eq!(second.left.as_identifier().text(), "orders");
		assert!(matches!(second.operator, InfixOperator::AccessTable(_)));
		assert_eq!(second.right.as_identifier().text(), "user_id");
	}

	#[test]
	fn test_left_join_with_alias() {
		let tokens = tokenize("left join { from test.customers } as c using (id, c.customer_id)").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result[0].first_unchecked().as_join();

		let AstJoin::LeftJoin {
			with,
			using_clause,
			alias,
			..
		} = &result
		else {
			panic!("Expected LeftJoin");
		};

		// Check alias
		assert_eq!(alias.text(), "c");

		// Check joined table
		let first_node = with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
			assert_eq!(source.name.text(), "customers");
		} else {
			panic!("Expected From node in subquery");
		}

		// Check using clause
		assert_eq!(using_clause.pairs.len(), 1);

		// First expression: id (unqualified - refers to current dataframe)
		let first = &using_clause.pairs[0].first;
		assert_eq!(first.as_identifier().text(), "id");

		// Second expression: c.customer_id (qualified with join alias)
		let second = using_clause.pairs[0].second.as_infix();
		assert_eq!(second.left.as_identifier().text(), "c");
		assert_eq!(second.right.as_identifier().text(), "customer_id");
	}

	#[test]
	fn test_complex_query_with_aliases() {
		// Test the full example query with aliases
		let tokens =
			tokenize("from test.orders left join { from test.customers } as c using (customer_id, c.id)")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 2); // FROM and LEFT JOIN nodes

		// Check FROM clause
		let from = statement.nodes[0].as_from();
		match from {
			crate::ast::AstFrom::Source {
				source,
				..
			} => {
				assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(source.name.text(), "orders");
			}
			_ => panic!("Expected Source"),
		}

		// Check LEFT JOIN with alias
		let join = statement.nodes[1].as_join();
		let AstJoin::LeftJoin {
			with,
			using_clause,
			alias,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		// Check alias
		assert_eq!(alias.text(), "c");

		// Check that the subquery contains "from test.customers"
		let first_node = with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
			assert_eq!(source.name.text(), "customers");
		} else {
			panic!("Expected From node in subquery");
		}

		// Check using clause
		assert_eq!(using_clause.pairs.len(), 1);

		// First expression: customer_id (unqualified - refers to current dataframe)
		let first = &using_clause.pairs[0].first;
		assert_eq!(first.as_identifier().text(), "customer_id");

		// Second expression: c.id (qualified with join alias)
		let second = using_clause.pairs[0].second.as_infix();
		assert_eq!(second.left.as_identifier().text(), "c");
		assert_eq!(second.right.as_identifier().text(), "id");
	}

	#[test]
	fn test_left_join_with_multiple_conditions() {
		let tokens = tokenize("left join { from orders } as o using (id, o.user_id) and (tenant, o.tenant)")
			.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::LeftJoin {
			with,
			using_clause,
			alias,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		// Check alias
		assert_eq!(alias.text(), "o");

		// Check that the subquery contains "from orders"
		let first_node = with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.name.text(), "orders");
		} else {
			panic!("Expected From node in subquery");
		}

		// Check using clause has two pairs
		assert_eq!(using_clause.pairs.len(), 2);

		// First pair: id (unqualified), o.user_id (qualified with alias)
		let pair1_first = &using_clause.pairs[0].first;
		assert_eq!(pair1_first.as_identifier().text(), "id");

		let pair1_second = using_clause.pairs[0].second.as_infix();
		assert_eq!(pair1_second.left.as_identifier().text(), "o");
		assert_eq!(pair1_second.right.as_identifier().text(), "user_id");

		// Second pair: tenant (unqualified), o.tenant (qualified with alias)
		let pair2_first = &using_clause.pairs[1].first;
		assert_eq!(pair2_first.as_identifier().text(), "tenant");

		let pair2_second = using_clause.pairs[1].second.as_infix();
		assert_eq!(pair2_second.left.as_identifier().text(), "o");
		assert_eq!(pair2_second.right.as_identifier().text(), "tenant");
	}

	#[test]
	fn test_left_join_with_or_connector() {
		let tokens =
			tokenize("left join { from orders } as o using (id, o.user_id) or (tenant, o.tenant)").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::LeftJoin {
			using_clause,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		// Check using clause has two pairs (connected with 'or')
		assert_eq!(using_clause.pairs.len(), 2);
	}

	#[test]
	fn test_using_with_literal_expression() {
		let tokens = tokenize("left join { from orders } as o using (id, o.type) and (category, 123)").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result[0].first_unchecked().as_join();

		let AstJoin::LeftJoin {
			using_clause,
			..
		} = &result
		else {
			panic!("Expected LeftJoin");
		};

		assert_eq!(using_clause.pairs.len(), 2);

		// First pair: id (unqualified), o.type (qualified with alias)
		let first = &using_clause.pairs[0].first;
		assert_eq!(first.as_identifier().text(), "id");
		let second = using_clause.pairs[0].second.as_infix();
		assert_eq!(second.left.as_identifier().text(), "o");
		assert_eq!(second.right.as_identifier().text(), "type");

		// Second pair: category (unqualified), literal 123
		let pair2_first = &using_clause.pairs[1].first;
		assert_eq!(pair2_first.as_identifier().text(), "category");
		let pair2_second = &using_clause.pairs[1].second;
		assert!(matches!(**pair2_second, Ast::Literal(AstLiteral::Number(_))));
	}

	#[test]
	fn test_natural_join_simple() {
		let tokens = tokenize("natural join { from orders } as o").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				with,
				join_type,
				alias,
				..
			} => {
				let first_node = with.statement.nodes.first().expect("Expected node in subquery");
				if let Ast::From(AstFrom::Source {
					source,
					..
				}) = first_node
				{
					assert_eq!(source.name.text(), "orders");
				} else {
					panic!("Expected From node in subquery");
				}
				assert_eq!(join_type, &None);
				assert_eq!(alias.text(), "o");
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_join_with_alias() {
		let tokens = tokenize("natural join { from orders } as ord").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				alias,
				..
			} => {
				assert_eq!(alias.text(), "ord");
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_left_join() {
		let tokens = tokenize("natural left join { from orders } as o").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				join_type,
				alias,
				..
			} => {
				assert_eq!(join_type, &Some(JoinType::Left));
				assert_eq!(alias.text(), "o");
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_inner_join() {
		let tokens = tokenize("natural inner join { from orders } as o").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				join_type,
				alias,
				..
			} => {
				assert_eq!(join_type, &Some(JoinType::Inner));
				assert_eq!(alias.text(), "o");
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_inner_join_with_using() {
		let tokens = tokenize("inner join { from orders } as o using (id, o.user_id)").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::InnerJoin {
			with,
			using_clause,
			alias,
			..
		} = &join
		else {
			panic!("Expected InnerJoin");
		};

		// Check alias
		assert_eq!(alias.text(), "o");

		// Check that the subquery contains "from orders"
		let first_node = with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.name.text(), "orders");
		} else {
			panic!("Expected From node in subquery");
		}

		assert_eq!(using_clause.pairs.len(), 1);
	}

	#[test]
	fn test_join_implicit_inner_with_using() {
		let tokens = tokenize("join { from orders } as o using (id, o.user_id)").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::InnerJoin {
			using_clause,
			alias,
			..
		} = &join
		else {
			panic!("Expected InnerJoin");
		};

		assert_eq!(alias.text(), "o");
		assert_eq!(using_clause.pairs.len(), 1);
	}
}
