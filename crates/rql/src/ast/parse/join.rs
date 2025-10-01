// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{JoinStrategy, JoinType};
use reifydb_type::{
	diagnostic::ast::{multiple_expressions_without_braces, unexpected_token_error},
	return_error,
};

use crate::ast::{
	AstJoin,
	parse::{Parser, Precedence},
	tokenize::{
		Keyword::{Inner, Join, Left, Natural, On, With},
		Operator::{CloseCurly, Colon, OpenCurly},
		Separator::Comma,
	},
};

impl<'a> Parser<'a> {
	// Parse the WITH { strategy: ... } clause
	pub(crate) fn parse_join_strategy(&mut self) -> crate::Result<Option<JoinStrategy>> {
		if !self.is_eof() && self.current()?.is_keyword(With) {
			self.advance()?; // consume 'with'
			self.consume_operator(OpenCurly)?;

			// Expect 'strategy' identifier
			if !self.current()?.is_identifier() || self.current()?.fragment.text() != "strategy" {
				return_error!(unexpected_token_error("strategy", self.current()?.fragment.clone()));
			}
			self.advance()?;

			self.consume_operator(Colon)?;

			// Parse the strategy value
			let strategy = if self.current()?.is_identifier() {
				match self.current()?.fragment.text() {
					"lazy_loading" => {
						self.advance()?;
						JoinStrategy::LazyLoading
					}
					"eager_loading" => {
						self.advance()?;
						JoinStrategy::EagerLoading
					}
					_ => {
						return_error!(unexpected_token_error(
							"lazy_loading or eager_loading",
							self.current()?.fragment.clone()
						));
					}
				}
			} else {
				return_error!(unexpected_token_error(
					"strategy value",
					self.current()?.fragment.clone()
				));
			};

			self.consume_operator(CloseCurly)?;
			Ok(Some(strategy))
		} else {
			Ok(None)
		}
	}

	pub(crate) fn parse_join(&mut self) -> crate::Result<AstJoin<'a>> {
		let token = self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		// Check for alias before 'on' keyword
		let alias = if !self.is_eof() && self.current()?.is_identifier() {
			let alias_token = self.advance()?;
			Some(alias_token.fragment)
		} else {
			None
		};

		self.consume_keyword(On)?;

		let has_on_braces = self.current()?.is_operator(OpenCurly);

		if has_on_braces {
			self.advance()?;
		}

		let mut on = Vec::new();
		loop {
			on.push(self.parse_node(Precedence::None)?);

			if self.is_eof() {
				break;
			}

			if has_on_braces && self.current()?.is_operator(CloseCurly) {
				self.advance()?;
				break;
			}

			if self.current()?.is_separator(Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		if on.len() > 1 && !has_on_braces {
			return_error!(multiple_expressions_without_braces(token.fragment));
		}

		let strategy = self.parse_join_strategy()?;

		Ok(AstJoin::InnerJoin {
			token,
			with,
			on,
			alias,
			strategy,
		})
	}

	pub(crate) fn parse_natural_join(&mut self) -> crate::Result<AstJoin<'a>> {
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

		// Check for alias after the join clause
		let alias = if !self.is_eof() && self.current().is_ok() && self.current()?.is_identifier() {
			let alias_token = self.advance()?;
			Some(alias_token.fragment)
		} else {
			None
		};

		let strategy = self.parse_join_strategy()?;

		Ok(AstJoin::NaturalJoin {
			token,
			with,
			join_type,
			alias,
			strategy,
		})
	}

	pub(crate) fn parse_inner_join(&mut self) -> crate::Result<AstJoin<'a>> {
		let token = self.consume_keyword(Inner)?;
		self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		// Check for alias before 'on' keyword
		let alias = if !self.is_eof() && self.current()?.is_identifier() {
			let alias_token = self.advance()?;
			Some(alias_token.fragment)
		} else {
			None
		};

		self.consume_keyword(On)?;

		let has_on_braces = self.current()?.is_operator(OpenCurly);

		if has_on_braces {
			self.advance()?;
		}

		let mut on = Vec::new();
		loop {
			on.push(self.parse_node(Precedence::None)?);

			if self.is_eof() {
				break;
			}

			if has_on_braces && self.current()?.is_operator(CloseCurly) {
				self.advance()?;
				break;
			}

			if self.current()?.is_separator(Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		if on.len() > 1 && !has_on_braces {
			return_error!(multiple_expressions_without_braces(token.fragment));
		}

		// Parse optional WITH clause for strategy
		let strategy = self.parse_join_strategy()?;

		Ok(AstJoin::InnerJoin {
			token,
			with,
			on,
			alias,
			strategy,
		})
	}

	pub(crate) fn parse_left_join(&mut self) -> crate::Result<AstJoin<'a>> {
		let token = self.consume_keyword(Left)?;
		self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		// Check for alias before 'on' keyword
		let alias = if !self.is_eof() && self.current()?.is_identifier() {
			let alias_token = self.advance()?;
			Some(alias_token.fragment)
		} else {
			None
		};

		self.consume_keyword(On)?;

		let has_on_braces = self.current()?.is_operator(OpenCurly);

		if has_on_braces {
			self.advance()?;
		}

		let mut on = Vec::new();
		loop {
			on.push(self.parse_node(Precedence::None)?);

			if self.is_eof() {
				break;
			}

			if has_on_braces && self.current()?.is_operator(CloseCurly) {
				self.advance()?;
				break;
			}

			if self.current()?.is_separator(Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		if on.len() > 1 && !has_on_braces {
			return_error!(multiple_expressions_without_braces(token.fragment));
		}

		// Parse optional WITH clause for strategy
		let strategy = self.parse_join_strategy()?;

		Ok(AstJoin::LeftJoin {
			token,
			with,
			on,
			alias,
			strategy,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{JoinStrategy, JoinType};

	use crate::ast::{Ast, AstFrom, AstJoin, InfixOperator, parse::Parser, tokenize::tokenize};

	#[test]
	fn test_left_join_with_strategy() {
		let tokens = tokenize(
			"left join { from test.orders } on user_id == order_id with { strategy: lazy_loading }",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		let join = result[0].first_unchecked().as_join();
		let AstJoin::LeftJoin {
			strategy,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		assert_eq!(strategy, &Some(JoinStrategy::LazyLoading));
	}

	#[test]
	fn test_left_join_with_eager_strategy() {
		let tokens = tokenize(
			"left join { from test.orders } on user_id == order_id with { strategy: eager_loading }",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		let join = result[0].first_unchecked().as_join();
		let AstJoin::LeftJoin {
			strategy,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		assert_eq!(strategy, &Some(JoinStrategy::EagerLoading));
	}

	#[test]
	fn test_left_join_without_strategy() {
		let tokens = tokenize("left join { from test.orders } on user_id == order_id").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		let join = result[0].first_unchecked().as_join();
		let AstJoin::LeftJoin {
			strategy,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		assert_eq!(strategy, &None);
	}

	#[test]
	fn test_left_join() {
		let tokens = tokenize("left join { from namespace.orders } on user.id == orders.user_id").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::LeftJoin {
			with,
			on,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};
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

		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();
		{
			let left = on.left.as_infix();
			assert_eq!(left.left.as_identifier().text(), "user");
			assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
			assert_eq!(left.right.as_identifier().text(), "id");
		}

		assert!(matches!(on.operator, InfixOperator::Equal(_)));

		{
			let right = on.right.as_infix();
			assert_eq!(right.left.as_identifier().text(), "orders");
			assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
			assert_eq!(right.right.as_identifier().text(), "user_id");
		}
	}

	#[test]
	fn test_left_join_with_alias() {
		let tokens = tokenize("left join { from test.customers } c on users.id == c.customer_id").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result[0].first_unchecked().as_join();

		let AstJoin::LeftJoin {
			with,
			on,
			alias,
			..
		} = &result
		else {
			panic!("Expected LeftJoin");
		};

		// Check alias
		assert_eq!(alias.as_ref().unwrap().text(), "c");

		// Check joined table
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

		// Check ON condition
		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();

		// Left side: users.id
		let left = on.left.as_infix();
		assert_eq!(left.left.as_identifier().text(), "users");
		assert_eq!(left.right.as_identifier().text(), "id");

		// Right side: c.customer_id
		let right = on.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "c");
		assert_eq!(right.right.as_identifier().text(), "customer_id");
	}

	#[test]
	fn test_complex_query_with_aliases() {
		// Test the full example query with aliases
		let tokens = tokenize(
			"from test.orders o left join { from test.customers } c on o.customer_id == c.customer_id",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1); // This is parsed as one statement with multiple nodes

		// The result is one statement with multiple nodes
		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 2); // FROM and LEFT JOIN nodes

		// Check FROM clause with alias
		let from = statement.nodes[0].as_from();
		match from {
			crate::ast::AstFrom::Source {
				source,
				..
			} => {
				assert_eq!(source.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(source.name.text(), "orders");
				assert_eq!(source.alias.as_ref().unwrap().text(), "o");
			}
			_ => panic!("Expected Source"),
		}

		// Check LEFT JOIN with alias
		let join = statement.nodes[1].as_join();
		let AstJoin::LeftJoin {
			with,
			on,
			alias,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};

		// Check alias
		assert_eq!(alias.as_ref().unwrap().text(), "c");

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

		// Check ON condition uses aliases
		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();

		// Left side: o.customer_id
		let left = on.left.as_infix();
		assert_eq!(left.left.as_identifier().text(), "o");
		assert_eq!(left.right.as_identifier().text(), "customer_id");

		// Right side: c.customer_id
		let right = on.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "c");
		assert_eq!(right.right.as_identifier().text(), "customer_id");
	}

	#[test]
	fn test_left_join_with_curly() {
		let tokens = tokenize(
			"left join { from orders } on { users.id == orders.user_id, something_else.id == orders.user_id }",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::LeftJoin {
			with,
			on,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};
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

		assert_eq!(on.len(), 2);

		// First condition: users.id == orders.user_id
		let on1 = on[0].as_infix();
		{
			let left = on1.left.as_infix();
			assert_eq!(left.left.as_identifier().text(), "users");
			assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
			assert_eq!(left.right.as_identifier().text(), "id");
		}
		assert!(matches!(on1.operator, InfixOperator::Equal(_)));
		{
			let right = on1.right.as_infix();
			assert_eq!(right.left.as_identifier().text(), "orders");
			assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
			assert_eq!(right.right.as_identifier().text(), "user_id");
		}

		// Second condition: something_else.id == orders.user_id
		let on2 = on[1].as_infix();
		{
			let left = on2.left.as_infix();
			assert_eq!(left.left.as_identifier().text(), "something_else");
			assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
			assert_eq!(left.right.as_identifier().text(), "id");
		}
		assert!(matches!(on2.operator, InfixOperator::Equal(_)));
		{
			let right = on2.right.as_infix();
			assert_eq!(right.left.as_identifier().text(), "orders");
			assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
			assert_eq!(right.right.as_identifier().text(), "user_id");
		}
	}

	#[test]
	fn test_left_join_single_on_with_braces() {
		let tokens = tokenize("left join { from orders } on { users.id == orders.user_id }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::LeftJoin {
			with,
			on,
			..
		} = &join
		else {
			panic!("Expected LeftJoin");
		};
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
		assert_eq!(on.len(), 1);
	}

	#[test]
	fn test_left_join_multiple_on_without_braces_fails() {
		let tokens = tokenize(
			"left join { from orders } on users.id == orders.user_id, something_else.id == orders.user_id",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(result.is_err(), "Expected error for multiple ON conditions without braces");
	}

	#[test]
	fn test_natural_join_simple() {
		let tokens = tokenize("natural join { from orders }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				with,
				join_type,
				..
			} => {
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
				assert_eq!(join_type, &None);
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_join_with_qualified_table() {
		let tokens = tokenize("natural join { from namespace.orders }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				with,
				join_type,
				..
			} => {
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
				assert_eq!(join_type, &None);
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_join_with_braces() {
		let tokens = tokenize("natural join { from orders }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				with,
				join_type,
				..
			} => {
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
				assert_eq!(join_type, &None);
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_left_join() {
		let tokens = tokenize("natural left join { from orders }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				with,
				join_type,
				..
			} => {
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
				assert_eq!(join_type, &Some(JoinType::Left));
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_inner_join() {
		let tokens = tokenize("natural inner join { from orders }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		match &join {
			AstJoin::NaturalJoin {
				with,
				join_type,
				..
			} => {
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
				assert_eq!(join_type, &Some(JoinType::Inner));
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_inner_join() {
		let tokens = tokenize("inner join { from orders } on users.id == orders.user_id").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::InnerJoin {
			with,
			on,
			..
		} = &join
		else {
			panic!("Expected InnerJoin");
		};
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

		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();
		{
			let left = on.left.as_infix();
			assert_eq!(left.left.as_identifier().text(), "users");
			assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
			assert_eq!(left.right.as_identifier().text(), "id");
		}

		assert!(matches!(on.operator, InfixOperator::Equal(_)));

		{
			let right = on.right.as_infix();
			assert_eq!(right.left.as_identifier().text(), "orders");
			assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
			assert_eq!(right.right.as_identifier().text(), "user_id");
		}
	}

	#[test]
	fn test_join() {
		let tokens = tokenize("join { from orders } on users.id == orders.user_id").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let join = result.first_unchecked().as_join();

		let AstJoin::InnerJoin {
			with,
			on,
			..
		} = &join
		else {
			panic!("Expected InnerJoin");
		};
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

		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();
		{
			let left = on.left.as_infix();
			assert_eq!(left.left.as_identifier().text(), "users");
			assert!(matches!(left.operator, InfixOperator::AccessTable(_)));
			assert_eq!(left.right.as_identifier().text(), "id");
		}

		assert!(matches!(on.operator, InfixOperator::Equal(_)));

		{
			let right = on.right.as_infix();
			assert_eq!(right.left.as_identifier().text(), "orders");
			assert!(matches!(right.operator, InfixOperator::AccessTable(_)));
			assert_eq!(right.right.as_identifier().text(), "user_id");
		}
	}
}
