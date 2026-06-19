// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::JoinType;

use crate::{
	Result,
	ast::{
		ast::{AstJoin, AstJoinExpressionPair, AstUsingClause, JoinConnector},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	diagnostic::AstError,
	token::{
		keyword::Keyword::{Inner, Join, Left, Natural, Using},
		operator::Operator::{And, As, CloseParen, OpenParen, Or},
		separator::Separator::Comma,
		token::TokenKind,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_join(&mut self) -> Result<AstJoin<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(Join)?;
		let with = self.parse_sub_query()?;

		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;

		let using_clause = self.parse_using_clause()?;
		let (ttl, snapshot, latest) = self.parse_with_clause_for_join()?;

		Ok(AstJoin::InnerJoin {
			token,
			with,
			using_clause,
			alias,
			ttl,
			snapshot,
			latest,
			rql: self.source_since(start),
		})
	}

	pub(crate) fn parse_natural_join(&mut self) -> Result<AstJoin<'bump>> {
		let start = self.current()?.fragment.offset();
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

		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;
		let (ttl, snapshot, latest) = self.parse_with_clause_for_join()?;

		Ok(AstJoin::NaturalJoin {
			token,
			with,
			join_type,
			alias,
			ttl,
			snapshot,
			latest,
			rql: self.source_since(start),
		})
	}

	pub(crate) fn parse_inner_join(&mut self) -> Result<AstJoin<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(Inner)?;
		self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;

		let using_clause = self.parse_using_clause()?;
		let (ttl, snapshot, latest) = self.parse_with_clause_for_join()?;

		Ok(AstJoin::InnerJoin {
			token,
			with,
			using_clause,
			alias,
			ttl,
			snapshot,
			latest,
			rql: self.source_since(start),
		})
	}

	pub(crate) fn parse_left_join(&mut self) -> Result<AstJoin<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(Left)?;
		self.consume_keyword(Join)?;

		let with = self.parse_sub_query()?;

		self.consume_operator(As)?;
		let alias = self.consume(TokenKind::Identifier)?.fragment;

		let using_clause = self.parse_using_clause()?;
		let (ttl, snapshot, latest) = self.parse_with_clause_for_join()?;

		Ok(AstJoin::LeftJoin {
			token,
			with,
			using_clause,
			alias,
			ttl,
			snapshot,
			latest,
			rql: self.source_since(start),
		})
	}

	fn parse_using_clause(&mut self) -> Result<AstUsingClause<'bump>> {
		let using_token = self.consume_keyword(Using)?;
		let mut pairs = Vec::new();

		loop {
			self.consume_operator(OpenParen)?;
			let first = self.parse_node(Precedence::None)?;

			if !self.current()?.is_separator(Comma) {
				return Err(AstError::TokenizeError {
					message: "expected ','".to_string(),
				}
				.into());
			}
			self.advance()?;
			let second = self.parse_node(Precedence::None)?;
			self.consume_operator(CloseParen)?;

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
				first: BumpBox::new_in(first, self.bump()),
				second: BumpBox::new_in(second, self.bump()),
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
pub mod tests {
	use reifydb_core::common::JoinType;

	use crate::{
		ast::{
			ast::{Ast, AstFrom, AstJoin, AstLiteral, InfixOperator},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_left_join_with_using() {
		let bump = Bump::new();
		let source = "left join { from namespace::orders } as orders using (id, orders.user_id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
			assert_eq!(source.namespace[0].text(), "namespace");
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
		let bump = Bump::new();
		let source = "left join { from test::customers } as c using (id, c.customer_id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
			assert_eq!(source.namespace[0].text(), "test");
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
		let bump = Bump::new();
		// Test the full example query with aliases
		let source = "from test::orders left join { from test::customers } as c using (customer_id, c.id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.nodes.len(), 2); // FROM and LEFT JOIN nodes

		// Check FROM clause
		let from = statement.nodes[0].as_from();
		match from {
			AstFrom::Source {
				source,
				..
			} => {
				assert_eq!(source.namespace[0].text(), "test");
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

		// Check that the subquery contains "from test::customers"
		let first_node = with.statement.nodes.first().expect("Expected node in subquery");
		if let Ast::From(AstFrom::Source {
			source,
			..
		}) = first_node
		{
			assert_eq!(source.namespace[0].text(), "test");
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
		let bump = Bump::new();
		let source = "left join { from orders } as o using (id, o.user_id) and (tenant, o.tenant)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "left join { from orders } as o using (id, o.user_id) or (tenant, o.tenant)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "left join { from orders } as o using (id, o.type) and (category, 123)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "natural join { from orders } as o";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "natural join { from orders } as ord";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "natural left join { from orders } as o";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "natural inner join { from orders } as o";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let bump = Bump::new();
		let source = "join { from orders } as o using (id, o.user_id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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

	#[test]
	fn test_inner_join_with_ttl_both_sides() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id) \
			with { ttl: { left: { duration: '1h' }, right: { duration: '2d' } } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		let result = result.pop().unwrap();
		let AstJoin::InnerJoin {
			ttl,
			..
		} = result.first_unchecked().as_join()
		else {
			panic!("Expected InnerJoin");
		};
		let ttl = ttl.as_ref().expect("expected ttl block");
		let left = ttl.left.as_ref().expect("left side present");
		assert_eq!(left.duration.fragment.text(), "1h");
		let right = ttl.right.as_ref().expect("right side present");
		assert_eq!(right.duration.fragment.text(), "2d");
	}

	#[test]
	fn test_inner_join_with_ttl_only_left() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id) \
			with { ttl: { left: { duration: '10m', on: updated } } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		let result = result.pop().unwrap();
		let AstJoin::InnerJoin {
			ttl,
			..
		} = result.first_unchecked().as_join()
		else {
			panic!("Expected InnerJoin");
		};
		let ttl = ttl.as_ref().expect("expected ttl block");
		let left = ttl.left.as_ref().expect("left present");
		assert_eq!(left.duration.fragment.text(), "10m");
		assert_eq!(left.anchor.as_ref().unwrap().fragment.text(), "updated");
		assert!(ttl.right.is_none(), "right side must be absent when only left is given");
	}

	#[test]
	fn test_left_join_with_ttl_only_right() {
		let bump = Bump::new();
		let source = "left join { from orders } as o using (id, o.user_id) \
			with { ttl: { right: { duration: '1d' } } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		let result = result.pop().unwrap();
		let AstJoin::LeftJoin {
			ttl,
			..
		} = result.first_unchecked().as_join()
		else {
			panic!("Expected LeftJoin");
		};
		let ttl = ttl.as_ref().expect("expected ttl block");
		assert!(ttl.left.is_none());
		assert_eq!(ttl.right.as_ref().unwrap().duration.fragment.text(), "1d");
	}

	#[test]
	fn test_join_with_ttl_empty_body_rejected() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id) with { ttl: { } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err(), "expected error for empty join ttl body");
	}

	#[test]
	fn test_join_with_old_single_ttl_shorthand_rejected() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id) \
			with { ttl: { duration: '1h' } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(
			result.is_err(),
			"expected error for legacy shorthand: ttl on join now requires explicit 'left'/'right' keys"
		);
	}

	#[test]
	fn test_join_with_unknown_side_key_rejected() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id) \
			with { ttl: { middle: { duration: '1h' } } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();
		assert!(result.is_err(), "expected error for unknown side key in join ttl");
	}

	#[test]
	fn test_join_with_ttl_and_snapshot() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id) \
			with { ttl: { left: { duration: '5m' } }, snapshot: true }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		let result = result.pop().unwrap();
		let AstJoin::InnerJoin {
			ttl,
			snapshot,
			..
		} = result.first_unchecked().as_join()
		else {
			panic!("Expected InnerJoin");
		};
		assert!(*snapshot, "snapshot flag should still parse alongside per-side ttl");
		let ttl = ttl.as_ref().expect("expected ttl");
		assert!(ttl.left.is_some());
		assert!(ttl.right.is_none());
	}

	#[test]
	fn test_join_with_latest_flag() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id) \
			with { snapshot: true, latest: true, ttl: { left: { duration: '10s', on: created, mode: drop } } }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		let result = result.pop().unwrap();
		let AstJoin::InnerJoin {
			snapshot,
			latest,
			..
		} = result.first_unchecked().as_join()
		else {
			panic!("Expected InnerJoin");
		};
		assert!(*snapshot, "snapshot must parse alongside latest");
		assert!(*latest, "latest flag must parse from the join WITH clause");
	}

	#[test]
	fn test_join_latest_defaults_false() {
		let bump = Bump::new();
		let source = "inner join { from orders } as o using (id, o.user_id)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		let result = result.pop().unwrap();
		let AstJoin::InnerJoin {
			latest,
			..
		} = result.first_unchecked().as_join()
		else {
			panic!("Expected InnerJoin");
		};
		assert!(!*latest, "latest must default to false when omitted");
	}
}
