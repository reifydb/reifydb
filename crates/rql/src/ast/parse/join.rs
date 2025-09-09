// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::JoinType;
use reifydb_type::{
	diagnostic::ast::multiple_expressions_without_braces, return_error,
};

use crate::ast::{
	AstJoin,
	parse::{Parser, Precedence},
	tokenize::{
		Keyword::{From, Inner, Join, Left, Natural, On},
		Operator::{CloseCurly, OpenCurly},
		Separator::Comma,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_join(&mut self) -> crate::Result<AstJoin<'a>> {
		let token = self.consume_keyword(Join)?;

		self.consume_operator(OpenCurly)?;
		self.consume_keyword(From)?;
		let with = Box::new(self.parse_node(Precedence::None)?);
		self.consume_operator(CloseCurly)?;

		// Check for alias before 'on' keyword
		let alias = if !self.is_eof() && self.current()?.is_identifier()
		{
			let alias_token = self.advance()?;
			Some(crate::ast::AstIdentifier(alias_token))
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

			if has_on_braces
				&& self.current()?.is_operator(CloseCurly)
			{
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
			return_error!(multiple_expressions_without_braces(
				token.fragment
			));
		}

		Ok(AstJoin::InnerJoin {
			token,
			with,
			on,
			alias,
		})
	}

	pub(crate) fn parse_natural_join(
		&mut self,
	) -> crate::Result<AstJoin<'a>> {
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

		self.consume_operator(OpenCurly)?;
		self.consume_keyword(From)?;
		let with = Box::new(self.parse_node(Precedence::None)?);
		self.consume_operator(CloseCurly)?;

		// Check for alias after the join clause
		let alias = if !self.is_eof()
			&& self.current().is_ok()
			&& self.current()?.is_identifier()
		{
			let alias_token = self.advance()?;
			Some(crate::ast::AstIdentifier(alias_token))
		} else {
			None
		};

		Ok(AstJoin::NaturalJoin {
			token,
			with,
			join_type,
			alias,
		})
	}

	pub(crate) fn parse_inner_join(
		&mut self,
	) -> crate::Result<AstJoin<'a>> {
		let token = self.consume_keyword(Inner)?;
		self.consume_keyword(Join)?;

		self.consume_operator(OpenCurly)?;
		self.consume_keyword(From)?;
		let with = Box::new(self.parse_node(Precedence::None)?);
		self.consume_operator(CloseCurly)?;

		// Check for alias before 'on' keyword
		let alias = if !self.is_eof() && self.current()?.is_identifier()
		{
			let alias_token = self.advance()?;
			Some(crate::ast::AstIdentifier(alias_token))
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

			if has_on_braces
				&& self.current()?.is_operator(CloseCurly)
			{
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
			return_error!(multiple_expressions_without_braces(
				token.fragment
			));
		}

		Ok(AstJoin::InnerJoin {
			token,
			with,
			on,
			alias,
		})
	}

	pub(crate) fn parse_left_join(&mut self) -> crate::Result<AstJoin<'a>> {
		let token = self.consume_keyword(Left)?;
		self.consume_keyword(Join)?;

		self.consume_operator(OpenCurly)?;
		self.consume_keyword(From)?;
		let with = Box::new(self.parse_node(Precedence::None)?);
		self.consume_operator(CloseCurly)?;

		// Check for alias before 'on' keyword
		let alias = if !self.is_eof() && self.current()?.is_identifier()
		{
			let alias_token = self.advance()?;
			Some(crate::ast::AstIdentifier(alias_token))
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

			if has_on_braces
				&& self.current()?.is_operator(CloseCurly)
			{
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
			return_error!(multiple_expressions_without_braces(
				token.fragment
			));
		}

		Ok(AstJoin::LeftJoin {
			token,
			with,
			on,
			alias,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::JoinType;

	use crate::ast::{
		AstJoin, InfixOperator, parse::Parser, tokenize::tokenize,
	};

	#[test]
	fn test_left_join() {
		let tokens = tokenize(
			"left join { from schema.orders } on user.id == orders.user_id",
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
		let with = with.as_infix();
		assert_eq!(with.left.as_identifier().value(), "schema");
		assert!(matches!(with.operator, InfixOperator::AccessTable(_)));
		assert_eq!(with.right.as_identifier().value(), "orders");

		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();
		{
			let left = on.left.as_infix();
			assert_eq!(left.left.as_identifier().value(), "user");
			assert!(matches!(
				left.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(left.right.as_identifier().value(), "id");
		}

		assert!(matches!(on.operator, InfixOperator::Equal(_)));

		{
			let right = on.right.as_infix();
			assert_eq!(
				right.left.as_identifier().value(),
				"orders"
			);
			assert!(matches!(
				right.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(
				right.right.as_identifier().value(),
				"user_id"
			);
		}
	}

	#[test]
	fn test_left_join_with_alias() {
		let tokens = tokenize(
			"left join { from test.customers } c on users.id == c.customer_id",
		)
		.unwrap();
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
		assert_eq!(alias.as_ref().unwrap().value(), "c");

		// Check joined table
		let with = with.as_infix();
		assert_eq!(with.left.as_identifier().value(), "test");
		assert!(matches!(with.operator, InfixOperator::AccessTable(_)));
		assert_eq!(with.right.as_identifier().value(), "customers");

		// Check ON condition
		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();

		// Left side: users.id
		let left = on.left.as_infix();
		assert_eq!(left.left.as_identifier().value(), "users");
		assert_eq!(left.right.as_identifier().value(), "id");

		// Right side: c.customer_id
		let right = on.right.as_infix();
		assert_eq!(right.left.as_identifier().value(), "c");
		assert_eq!(right.right.as_identifier().value(), "customer_id");
	}

	#[test]
	fn test_complex_query_with_aliases() {
		// Test the full example query with aliases
		let tokens = tokenize("from test.orders o left join { from test.customers } c on o.customer_id == c.customer_id").unwrap();
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
				schema,
				source,
				alias,
				..
			} => {
				assert_eq!(
					schema.as_ref().unwrap().value(),
					"test"
				);
				assert_eq!(source.value(), "orders");
				assert_eq!(
					alias.as_ref().unwrap().value(),
					"o"
				);
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
		assert_eq!(alias.as_ref().unwrap().value(), "c");

		// Check joined table (test.customers)
		let with = with.as_infix();
		assert_eq!(with.left.as_identifier().value(), "test");
		assert_eq!(with.right.as_identifier().value(), "customers");

		// Check ON condition uses aliases
		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();

		// Left side: o.customer_id
		let left = on.left.as_infix();
		assert_eq!(left.left.as_identifier().value(), "o");
		assert_eq!(left.right.as_identifier().value(), "customer_id");

		// Right side: c.customer_id
		let right = on.right.as_infix();
		assert_eq!(right.left.as_identifier().value(), "c");
		assert_eq!(right.right.as_identifier().value(), "customer_id");
	}

	#[test]
	fn test_left_join_with_curly() {
		let tokens = tokenize("left join { from orders } on { users.id == orders.user_id, something_else.id == orders.user_id }").unwrap();
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
		assert_eq!(with.as_identifier().value(), "orders");

		assert_eq!(on.len(), 2);

		// First condition: users.id == orders.user_id
		let on1 = on[0].as_infix();
		{
			let left = on1.left.as_infix();
			assert_eq!(left.left.as_identifier().value(), "users");
			assert!(matches!(
				left.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(left.right.as_identifier().value(), "id");
		}
		assert!(matches!(on1.operator, InfixOperator::Equal(_)));
		{
			let right = on1.right.as_infix();
			assert_eq!(
				right.left.as_identifier().value(),
				"orders"
			);
			assert!(matches!(
				right.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(
				right.right.as_identifier().value(),
				"user_id"
			);
		}

		// Second condition: something_else.id == orders.user_id
		let on2 = on[1].as_infix();
		{
			let left = on2.left.as_infix();
			assert_eq!(
				left.left.as_identifier().value(),
				"something_else"
			);
			assert!(matches!(
				left.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(left.right.as_identifier().value(), "id");
		}
		assert!(matches!(on2.operator, InfixOperator::Equal(_)));
		{
			let right = on2.right.as_infix();
			assert_eq!(
				right.left.as_identifier().value(),
				"orders"
			);
			assert!(matches!(
				right.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(
				right.right.as_identifier().value(),
				"user_id"
			);
		}
	}

	#[test]
	fn test_left_join_single_on_with_braces() {
		let tokens = tokenize(
			"left join { from orders } on { users.id == orders.user_id }",
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
		assert_eq!(with.as_identifier().value(), "orders");
		assert_eq!(on.len(), 1);
	}

	#[test]
	fn test_left_join_multiple_on_without_braces_fails() {
		let tokens = tokenize("left join { from orders } on users.id == orders.user_id, something_else.id == orders.user_id").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(
			result.is_err(),
			"Expected error for multiple ON conditions without braces"
		);
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
				assert_eq!(
					with.as_identifier().value(),
					"orders"
				);
				assert_eq!(join_type, &None);
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_join_with_qualified_table() {
		let tokens = tokenize("natural join { from schema.orders }")
			.unwrap();
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
				let with = with.as_infix();
				assert_eq!(
					with.left.as_identifier().value(),
					"schema"
				);
				assert!(matches!(
					with.operator,
					InfixOperator::AccessTable(_)
				));
				assert_eq!(
					with.right.as_identifier().value(),
					"orders"
				);
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
				assert_eq!(
					with.as_identifier().value(),
					"orders"
				);
				assert_eq!(join_type, &None);
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_left_join() {
		let tokens =
			tokenize("natural left join { from orders }").unwrap();
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
				assert_eq!(
					with.as_identifier().value(),
					"orders"
				);
				assert_eq!(join_type, &Some(JoinType::Left));
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_natural_inner_join() {
		let tokens =
			tokenize("natural inner join { from orders }").unwrap();
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
				assert_eq!(
					with.as_identifier().value(),
					"orders"
				);
				assert_eq!(join_type, &Some(JoinType::Inner));
			}
			_ => panic!("Expected NaturalJoin"),
		}
	}

	#[test]
	fn test_inner_join() {
		let tokens = tokenize(
			"inner join { from orders } on users.id == orders.user_id",
		)
		.unwrap();
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
		assert_eq!(with.as_identifier().value(), "orders");

		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();
		{
			let left = on.left.as_infix();
			assert_eq!(left.left.as_identifier().value(), "users");
			assert!(matches!(
				left.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(left.right.as_identifier().value(), "id");
		}

		assert!(matches!(on.operator, InfixOperator::Equal(_)));

		{
			let right = on.right.as_infix();
			assert_eq!(
				right.left.as_identifier().value(),
				"orders"
			);
			assert!(matches!(
				right.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(
				right.right.as_identifier().value(),
				"user_id"
			);
		}
	}

	#[test]
	fn test_join() {
		let tokens = tokenize(
			"join { from orders } on users.id == orders.user_id",
		)
		.unwrap();
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
		assert_eq!(with.as_identifier().value(), "orders");

		assert_eq!(on.len(), 1);
		let on = on[0].as_infix();
		{
			let left = on.left.as_infix();
			assert_eq!(left.left.as_identifier().value(), "users");
			assert!(matches!(
				left.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(left.right.as_identifier().value(), "id");
		}

		assert!(matches!(on.operator, InfixOperator::Equal(_)));

		{
			let right = on.right.as_infix();
			assert_eq!(
				right.left.as_identifier().value(),
				"orders"
			);
			assert!(matches!(
				right.operator,
				InfixOperator::AccessTable(_)
			));
			assert_eq!(
				right.right.as_identifier().value(),
				"user_id"
			);
		}
	}
}
